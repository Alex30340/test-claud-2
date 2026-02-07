import re
import logging
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

from page_validator import is_whey_product_page, is_bad_url, has_purchase_proof, is_article_page

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = 12.0
MAX_CANDIDATES_TO_TEST = 10

POSITIVE_URL_PATTERNS = {
    "whey": 10,
    "proteine-whey": 10,
    "isolate": 6,
    "isolat": 6,
    "native": 4,
    "/product/": 6,
    "/produit/": 6,
    "/products/": 4,
    "/produits/": 4,
    "/shop/": 4,
    "/boutique/": 4,
    "/acheter/": 4,
    "/achat/": 4,
}

NEGATIVE_URL_PATTERNS = {
    "blog": -10,
    "guide": -10,
    "comparatif": -10,
    "collections": -8,
    "category": -8,
    "categories": -8,
    "/tag/": -8,
    "/tags/": -8,
    "endurance": -10,
    "boisson": -10,
    "creatine": -10,
    "gainer": -8,
    "pre-workout": -8,
    "bcaa": -6,
    "vitamine": -8,
    "omega": -8,
    "collagene": -8,
    "search": -8,
    "recherche": -8,
    "/faq": -8,
    "/aide": -8,
    "/contact": -8,
    "/panier": -8,
    "/cart": -8,
    "/account": -8,
    "/compte": -8,
    "/login": -8,
    "/politique": -8,
    "/cgv": -8,
    "/mentions": -8,
}

POSITIVE_ANCHOR_PATTERNS = {
    "whey": 8,
    "isolate": 5,
    "native": 3,
    "protéine": 4,
    "proteine": 4,
    "ajouter au panier": 6,
    "acheter": 4,
    "commander": 4,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


def _score_candidate_url(url: str, anchor_text: str, title_text: str) -> int:
    score = 0
    url_lower = url.lower()
    anchor_lower = (anchor_text or "").lower()
    title_lower = (title_text or "").lower()

    for pattern, points in POSITIVE_URL_PATTERNS.items():
        if pattern in url_lower:
            score += points

    for pattern, points in NEGATIVE_URL_PATTERNS.items():
        if pattern in url_lower:
            score += points

    for pattern, points in POSITIVE_ANCHOR_PATTERNS.items():
        if pattern in anchor_lower or pattern in title_lower:
            score += points

    return score


def _fetch_page(url: str) -> tuple[str | None, int | None]:
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
            response = client.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
    except Exception as e:
        logger.debug(f"[RESOLVER] Fetch error for {url}: {e}")
        return None, None


def _extract_internal_links(html: str, base_url: str, prefer_product_paths: bool = False) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower().replace("www.", "")

    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        link_domain = parsed.netloc.lower().replace("www.", "")

        if link_domain != base_domain:
            continue

        normalized = full_url.rstrip("/").lower().split("?")[0].split("#")[0]
        if normalized in seen:
            continue
        seen.add(normalized)

        bad, _ = is_bad_url(full_url)
        if bad:
            continue

        anchor_text = a.get_text(strip=True)
        title_text = a.get("title", "")

        links.append({
            "url": full_url,
            "anchor": anchor_text[:100],
            "title": title_text[:100],
        })

    return links


def resolve_best_product_url(
    start_url: str,
    target_keywords: list[str] | None = None,
    start_html: str | None = None,
    start_page_type: str | None = None,
) -> dict:
    if target_keywords is None:
        target_keywords = ["whey", "isolate", "native"]

    result = {
        "start_url": start_url,
        "start_page_type": start_page_type or "unknown",
        "is_start_whey_product": False,
        "resolved_url": None,
        "resolution_method": None,
        "candidates_tested": 0,
        "candidates_top10": [],
        "reasons": [],
    }

    html = start_html
    if html is None:
        html, http_status = _fetch_page(start_url)
        if html is None:
            result["reasons"].append(f"fetch_failed:http_status={http_status}")
            logger.info(f"[RESOLVER] Cannot fetch start URL: {start_url} (status={http_status})")
            return result

    is_whey, whey_result = is_whey_product_page(html, start_url)
    if is_whey:
        result["is_start_whey_product"] = True
        result["resolved_url"] = start_url
        result["resolution_method"] = "start_url_is_whey"
        result["start_page_type"] = "product"
        result["reasons"].append(f"start_url already whey product ({whey_result['whey_signal_count']} signals)")
        logger.debug(f"[RESOLVER] Start URL is already whey product: {start_url}")
        return result

    detected_page_type = whey_result.get("page_type", "unknown")
    result["start_page_type"] = detected_page_type
    result["reasons"].append(
        f"start_url not whey (type={detected_page_type}): {whey_result.get('rejection_reason', 'unknown')}"
    )

    is_article = detected_page_type == "article"
    if is_article:
        result["reasons"].append("start_url is article page, searching for product links on same domain")

    internal_links = _extract_internal_links(html, start_url, prefer_product_paths=is_article)
    result["reasons"].append(f"found {len(internal_links)} internal links")

    scored_links = []
    for link in internal_links:
        score = _score_candidate_url(link["url"], link["anchor"], link["title"])
        scored_links.append({
            "url": link["url"],
            "anchor": link["anchor"],
            "title": link["title"],
            "score": score,
        })

    scored_links.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = scored_links[:MAX_CANDIDATES_TO_TEST]
    result["candidates_top10"] = top_candidates

    tested = 0
    for candidate in top_candidates:
        if candidate["score"] <= -5:
            continue

        tested += 1
        c_html, c_status = _fetch_page(candidate["url"])
        if c_html is None:
            candidate["test_result"] = f"fetch_failed:{c_status}"
            continue

        c_is_whey, c_whey_result = is_whey_product_page(c_html, candidate["url"])
        candidate["is_whey_product"] = c_is_whey
        candidate["whey_signals"] = c_whey_result.get("whey_signals", [])
        candidate["page_type"] = c_whey_result.get("page_type", "unknown")

        if c_is_whey:
            result["resolved_url"] = candidate["url"]
            result["resolution_method"] = "article_to_product" if is_article else "crawl_resolved"
            result["candidates_tested"] = tested
            result["reasons"].append(
                f"resolved via crawl: {candidate['url']} "
                f"(score={candidate['score']}, {c_whey_result['whey_signal_count']} whey signals)"
            )
            logger.info(f"[RESOLVER] Resolved {start_url} => {candidate['url']} (from {'article' if is_article else 'non-product'})")
            return result

        candidate["test_result"] = c_whey_result.get("rejection_reason", "not_whey")

    result["candidates_tested"] = tested
    result["reasons"].append(f"no whey product found among {tested} candidates tested")
    logger.info(f"[RESOLVER] No whey product found for {start_url} ({tested} tested)")
    return result


def resolve_url_debug(url: str) -> dict:
    result = resolve_best_product_url(url)

    start_html, start_status = _fetch_page(url)
    result["start_http_status"] = start_status

    if start_html:
        is_whey, whey_info = is_whey_product_page(start_html, url)
        result["start_whey_detail"] = whey_info
    else:
        result["start_whey_detail"] = None

    return result
