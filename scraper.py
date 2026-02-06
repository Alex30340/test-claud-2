import json
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from scoring import (
    calculate_price_score,
    calculate_nutrition_score,
    calculate_health_score,
    calculate_global_score,
)

logger = logging.getLogger(__name__)


class BraveAPIError(Exception):
    pass

SEARCH_QUERIES = [
    "whey protein 1kg prix EUR achat",
    "whey isolate prix fiche produit",
    "whey hydrolysate achat en ligne",
    "whey native francaise prix",
    "impact whey protein myprotein prix",
    "clear whey isolate prix achat",
    "optimum nutrition gold standard whey prix",
    "bulk powders whey protein prix",
    "scitec nutrition whey protein prix",
    "eric favre whey protein prix",
    "foodspring whey protein prix",
    "whey isolate sans sucralose prix",
    "whey proteine fabrication francaise",
]

EXCLUDED_DOMAINS = [
    "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "reddit.com", "forum", "blog", "wikipedia.org",
    "pinterest.com", "linkedin.com", "quora.com",
    "amazon.fr", "amazon.com",
]

EXCLUDED_PATH_KEYWORDS = [
    "blog", "forum", "article", "comparatif", "avis", "guide",
    "video", "news", "actualite",
]

REQUEST_DELAY = 0.3
HTTP_TIMEOUT = 8.0
MAX_WORKERS = 8

SWEETENERS = {
    "sucralose": ["sucralose", "splenda"],
    "acesulfame_k": ["acesulfame", "acésulfame", "acesulfame-k", "e950"],
    "aspartame": ["aspartame", "e951"],
}

WHEY_TYPES = {
    "hydrolysate": ["hydrolys", "hydrolyzed", "hydrolysee", "hydrolysée", "hydrolysat"],
    "isolate": ["isolate", "isolat"],
    "native": ["native", "whey native"],
    "concentrate": ["concentrate", "concentre", "concentré"],
}

ORIGIN_FR_PATTERNS = [
    r"fabriqu[ée]e?\s+en\s+france",
    r"made\s+in\s+france",
    r"origine\s+france",
    r"lait\s+d['']\s*origine\s+fran[cç]aise?",
    r"lait\s+fran[cç]ais",
    r"produit\s+en\s+france",
]


def is_product_url(url: str) -> bool:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()

    for excluded in EXCLUDED_DOMAINS:
        if excluded in domain:
            return False

    for keyword in EXCLUDED_PATH_KEYWORDS:
        if keyword in path:
            return False

    return True


def search_brave(api_key: str, query: str, count: int = 15) -> list[str]:
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }
    params = {
        "q": query,
        "count": count,
        "country": "FR",
        "search_lang": "fr",
    }

    try:
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            response = client.get(url, headers=headers, params=params)
            if response.status_code == 422:
                error_data = response.json()
                error_code = error_data.get("error", {}).get("code", "")
                error_detail = error_data.get("error", {}).get("detail", "")
                raise BraveAPIError(f"{error_code}: {error_detail}")
            response.raise_for_status()
            data = response.json()

        results = []
        for item in data.get("web", {}).get("results", []):
            page_url = item.get("url", "")
            if page_url and is_product_url(page_url):
                results.append(page_url)
        return results

    except BraveAPIError:
        raise
    except Exception as e:
        logger.error(f"Brave search error for '{query}': {e}")
        return []


def search_all_queries(api_key: str, progress_callback=None) -> list[str]:
    all_urls = []
    seen = set()

    for i, query in enumerate(SEARCH_QUERIES):
        if progress_callback:
            progress_callback(i, len(SEARCH_QUERIES), query)

        urls = search_brave(api_key, query)
        for u in urls:
            normalized = u.rstrip("/").lower()
            if normalized not in seen:
                seen.add(normalized)
                all_urls.append(u)

        time.sleep(REQUEST_DELAY)

    return all_urls


def extract_jsonld(soup: BeautifulSoup) -> dict | None:
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        return item
            elif isinstance(data, dict):
                if data.get("@type") == "Product":
                    return data
                if "@graph" in data:
                    for item in data["@graph"]:
                        if isinstance(item, dict) and item.get("@type") == "Product":
                            return item
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def parse_weight(text: str) -> float | None:
    if not text:
        return None
    text = text.lower().replace(",", ".")

    match = re.search(r"(\d+(?:\.\d+)?)\s*kg", text)
    if match:
        return float(match.group(1))

    match = re.search(r"(\d+(?:\.\d+)?)\s*g(?:r|ramme)?", text)
    if match:
        grams = float(match.group(1))
        if grams >= 100:
            return grams / 1000

    return None


def parse_price(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", ".").strip()
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1))
    return None


def parse_protein(text: str) -> float | None:
    if not text:
        return None
    text = text.lower().replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)\s*g?", text)
    if match:
        val = float(match.group(1))
        if 0 < val <= 100:
            return val
    return None


def extract_og_meta(soup: BeautifulSoup) -> dict:
    meta = {}
    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if prop and content:
            meta[prop.lower()] = content
    return meta


def extract_microdata(soup: BeautifulSoup) -> dict:
    data = {}
    for tag in soup.find_all(attrs={"itemprop": True}):
        prop = tag.get("itemprop", "")
        value = tag.get("content", "") or tag.get("value", "") or tag.get_text(strip=True)
        if prop and value:
            data[prop.lower()] = value
    return data


def find_ingredients_block(text: str) -> str | None:
    if not text:
        return None
    t = " ".join(text.split())
    m = re.search(r"(ingr[ée]dients?\s*[:\-]\s*)(.{40,800})", t, re.I)
    if m:
        return m.group(2)[:800]
    return None


def detect_sweeteners(text: str) -> dict:
    t = (text or "").lower()
    out = {}
    for key, variants in SWEETENERS.items():
        out[key] = any(v in t for v in variants)
    return out


def detect_whey_type(text: str) -> str:
    t = (text or "").lower()
    for wtype, variants in WHEY_TYPES.items():
        if any(v in t for v in variants):
            return wtype
    return "unknown"


def detect_made_in_france(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t, re.I) for p in ORIGIN_FR_PATTERNS)


def detect_aminogram(text: str) -> bool:
    t = (text or "").lower()
    return (
        "aminogram" in t
        or "profil en acides amin" in t
        or "profil d'acides amin" in t
        or ("leucine" in t and "isoleucine" in t and "valine" in t)
    )


def detect_bcaa(text: str) -> bool:
    t = (text or "").lower()
    return "bcaa" in t or "2:1:1" in t


def extract_product_data(url: str) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        jsonld = extract_jsonld(soup)
        og = extract_og_meta(soup)
        microdata = extract_microdata(soup)

        name = ""
        brand = ""
        price = None
        currency = "EUR"
        availability = ""

        if jsonld:
            name = jsonld.get("name", "")
            brand_data = jsonld.get("brand", {})
            brand = brand_data.get("name", "") if isinstance(brand_data, dict) else str(brand_data)

            offers = jsonld.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            price = parse_price(offers.get("price"))
            currency = offers.get("priceCurrency", "EUR")
            availability = offers.get("availability", "")
            if availability:
                availability = availability.split("/")[-1]

        if not name:
            name = og.get("og:title", "") or microdata.get("name", "")
        if not name:
            title_tag = soup.find("title")
            name = title_tag.get_text(strip=True) if title_tag else ""
        if not brand:
            brand = og.get("product:brand", "") or microdata.get("brand", "")
        if price is None:
            price = parse_price(og.get("product:price:amount", "")) or parse_price(microdata.get("price", ""))
        if not currency or currency == "EUR":
            currency = og.get("product:price:currency", "EUR") or microdata.get("pricecurrency", "EUR")
        if not availability:
            availability = og.get("product:availability", "") or microdata.get("availability", "")
            if availability:
                availability = availability.split("/")[-1]

        if not name or price is None:
            html_price = None
            for cls in ["price", "product-price", "prix", "current-price"]:
                el = soup.find(class_=re.compile(cls, re.I))
                if el:
                    html_price = parse_price(el.get_text())
                    if html_price:
                        break
            if html_price and price is None:
                price = html_price

        if not name and price is None:
            return None

        bad_titles = ["amazon.fr", "amazon", "decathlon", "cdiscount", "google"]
        if name and name.strip().lower() in bad_titles:
            return None
        if name and len(name.strip()) < 5:
            return None
        if not name or not name.strip():
            name = "Produit inconnu"

        all_text = (name + " " + (jsonld.get("description", "") if jsonld else "")).lower()

        weight = parse_weight(all_text)
        if not weight and jsonld:
            weight_str = jsonld.get("weight", "")
            if isinstance(weight_str, dict):
                weight_str = weight_str.get("value", "")
            weight = parse_weight(str(weight_str))
        if not weight:
            page_text = soup.get_text().lower()
            weight_match = re.search(r"(\d+(?:[.,]\d+)?)\s*kg", page_text)
            if weight_match:
                w = float(weight_match.group(1).replace(",", "."))
                if 0.1 <= w <= 25:
                    weight = w
            if not weight:
                weight_match = re.search(r"(\d{3,5})\s*g(?:r|ramme)?", page_text)
                if weight_match:
                    g = float(weight_match.group(1))
                    if 100 <= g <= 25000:
                        weight = g / 1000

        price_per_kg = None
        if price and weight and weight > 0:
            price_per_kg = round(price / weight, 2)

        protein_per_100g = None
        if jsonld:
            nutrition = jsonld.get("nutrition", {})
            if isinstance(nutrition, dict):
                protein_per_100g = parse_protein(nutrition.get("proteinContent", ""))

        if not protein_per_100g:
            page_text = soup.get_text().lower()
            patterns = [
                r"prot[ée]ines?\s*[:/]?\s*(\d+(?:[.,]\d+)?)\s*g",
                r"(\d+(?:[.,]\d+)?)\s*g\s*(?:de\s+)?prot[ée]ines?",
                r"protein\s*[:/]?\s*(\d+(?:[.,]\d+)?)\s*g",
            ]
            for pattern in patterns:
                match = re.search(pattern, page_text)
                if match:
                    val = float(match.group(1).replace(",", "."))
                    if 10 < val <= 100:
                        protein_per_100g = val
                        break

        page_full_text = soup.get_text(" ", strip=True)
        ingredients_text = find_ingredients_block(page_full_text)
        analysis_text = ingredients_text or page_full_text

        sweeteners = detect_sweeteners(analysis_text)
        whey_type = detect_whey_type(name + " " + (all_text or "") + " " + (analysis_text or ""))
        made_in_france = detect_made_in_france(page_full_text)
        has_aminogram = detect_aminogram(page_full_text)
        mentions_bcaa = detect_bcaa(page_full_text)

        price_score = calculate_price_score(price_per_kg)
        nutrition_score = calculate_nutrition_score(protein_per_100g)
        health_score = calculate_health_score(
            protein_per_100g=protein_per_100g,
            whey_type=whey_type,
            made_in_france=made_in_france,
            has_sucralose=sweeteners.get("sucralose", False),
            has_acesulfame_k=sweeteners.get("acesulfame_k", False),
            has_aspartame=sweeteners.get("aspartame", False),
            has_aminogram=has_aminogram,
            mentions_bcaa=mentions_bcaa,
        )
        global_score = calculate_global_score(price_per_kg, protein_per_100g, health_score)

        return {
            "nom": name,
            "marque": brand,
            "url": url,
            "prix": price,
            "devise": currency,
            "disponibilite": availability,
            "poids_kg": weight,
            "prix_par_kg": price_per_kg,
            "proteines_100g": protein_per_100g,
            "type_whey": whey_type,
            "made_in_france": made_in_france,
            "has_sucralose": sweeteners.get("sucralose", False),
            "has_acesulfame_k": sweeteners.get("acesulfame_k", False),
            "has_aspartame": sweeteners.get("aspartame", False),
            "has_aminogram": has_aminogram,
            "mentions_bcaa": mentions_bcaa,
            "score_prix": price_score,
            "score_nutrition": nutrition_score,
            "score_sante": health_score,
            "score_global": global_score,
            "date_recuperation": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    except Exception as e:
        logger.warning(f"Error extracting data from {url}: {e}")
        return None


def _extract_with_log(url: str) -> dict | None:
    try:
        return extract_product_data(url)
    except Exception as e:
        logger.warning(f"Parallel extraction error for {url}: {e}")
        return None


def scrape_products(api_key: str, progress_callback=None, status_callback=None) -> list[dict]:
    if status_callback:
        status_callback("Recherche de produits sur internet...")

    urls = search_all_queries(api_key, progress_callback)

    if status_callback:
        status_callback(f"{len(urls)} URLs de produits trouvees. Extraction en parallele...")

    products = []
    completed = 0
    total = len(urls)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_extract_with_log, url): url for url in urls}

        for future in as_completed(futures):
            completed += 1
            url = futures[future]

            if progress_callback:
                progress_callback(completed, total, f"Extraction: {urlparse(url).netloc}")

            result = future.result()
            if result:
                products.append(result)

    products.sort(key=lambda p: p.get("score_global") or -1, reverse=True)

    return products
