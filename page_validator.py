import json
import re
import logging
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BAD_URL_PATH_PATTERNS = [
    "/blog", "/forum", "/guide", "/category", "/categories",
    "/tag/", "/tags/", "/search", "/recherche",
    "/comparatif", "/avis/", "/test/", "/review",
    "/article/", "/articles/", "/news/", "/actualite",
    "/conseil/", "/faq/", "/aide/", "/help/",
    "/recette/", "/tutoriel/", "/dossier/", "/magazine/",
    "/editorial/", "/top-", "/classement/",
    "/versus/", "/vs-", "/comment-choisir",
    "/pourquoi-", "/difference-entre",
    "/post/", "/collections", "/pages/",
    "/wiki/", "/glossaire/", "/lexique/",
]

BAD_URL_EXTENSIONS = [
    ".pdf", ".xml", ".json", ".rss", ".atom",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".mp4", ".mp3", ".avi", ".mov",
]

BAD_URL_DOMAINS = [
    "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "reddit.com", "wikipedia.org",
    "pinterest.com", "linkedin.com", "quora.com",
    "amazon.fr", "amazon.com",
    "idealo.fr", "idealo.com",
    "decathlon.fr", "decathlon.com",
    "doctissimo.fr", "passeportsante.net", "sante.journaldesfemmes.fr",
    "marmiton.org", "femmeactuelle.fr", "20minutes.fr", "lequipe.fr",
    "lefigaro.fr", "lemonde.fr", "bfmtv.com",
    "google.com", "google.fr",
]

EDITORIAL_H1_PATTERNS = [
    r"\bcomparatif\b", r"\bguide\b", r"\btop\s+\d+", r"\bmeilleur(?:e?s)?\b",
    r"\bclassement\b", r"\bselection\b", r"\bavis\b", r"\btest\b",
    r"\bcomment\s+choisir\b", r"\bquelle?\s+whey\b", r"\bdifference\s+entre\b",
    r"\bversus\b", r"\bvs\b",
]


def is_bad_url(url: str) -> tuple[bool, str]:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()
    full = (path + "?" + parsed.query).lower() if parsed.query else path

    for bd in BAD_URL_DOMAINS:
        if bd in domain:
            return True, f"blocked_domain:{bd}"

    for ext in BAD_URL_EXTENSIONS:
        if path.endswith(ext):
            return True, f"bad_extension:{ext}"

    for pattern in BAD_URL_PATH_PATTERNS:
        if pattern in full:
            return True, f"bad_path:{pattern}"

    return False, ""


def extract_jsonld_product_offer(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html
    result = {"has_product": False, "has_offer": False, "has_price": False, "has_availability": False, "product_name": ""}

    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        products = _find_products_in_jsonld(data)
        for product in products:
            result["has_product"] = True
            result["product_name"] = product.get("name", "")

            offers = product.get("offers", {})
            offer_list = offers if isinstance(offers, list) else [offers] if isinstance(offers, dict) else []

            for offer in offer_list:
                if not isinstance(offer, dict):
                    continue

                offer_type = offer.get("@type", "")
                if isinstance(offer_type, list):
                    is_offer = any(t in ("Offer", "AggregateOffer") for t in offer_type)
                else:
                    is_offer = offer_type in ("Offer", "AggregateOffer", "")

                if is_offer or offer.get("price") is not None or offer.get("priceCurrency"):
                    result["has_offer"] = True

                price = offer.get("price")
                low_price = offer.get("lowPrice")
                if price is not None or low_price is not None:
                    price_spec = offer.get("priceSpecification", {})
                    if isinstance(price_spec, dict) and price_spec.get("price"):
                        result["has_price"] = True
                    elif price is not None:
                        try:
                            pval = float(str(price).replace(",", ".").replace(" ", ""))
                            if pval > 0:
                                result["has_price"] = True
                        except (ValueError, TypeError):
                            pass
                    if low_price is not None:
                        try:
                            lval = float(str(low_price).replace(",", ".").replace(" ", ""))
                            if lval > 0:
                                result["has_price"] = True
                        except (ValueError, TypeError):
                            pass

                avail = offer.get("availability", "")
                if avail:
                    result["has_availability"] = True

    return result


def _find_products_in_jsonld(data) -> list[dict]:
    products = []

    def _is_product(item):
        if not isinstance(item, dict):
            return False
        t = item.get("@type")
        if isinstance(t, str):
            return t == "Product"
        if isinstance(t, list):
            return "Product" in t
        return False

    if isinstance(data, list):
        for item in data:
            if _is_product(item):
                products.append(item)
    elif isinstance(data, dict):
        if _is_product(data):
            products.append(data)
        if "@graph" in data and isinstance(data["@graph"], list):
            for item in data["@graph"]:
                if _is_product(item):
                    products.append(item)
    return products


_ADD_TO_CART_PATTERNS = [
    re.compile(r"ajouter\s+au\s+panier", re.I),
    re.compile(r"ajout\s+au?\s+panier", re.I),
    re.compile(r"add\s+to\s+cart", re.I),
    re.compile(r"acheter", re.I),
    re.compile(r"commander", re.I),
    re.compile(r"ajouter\s+au\s+sac", re.I),
]

_ADD_TO_CART_CLASSES = re.compile(
    r"add.?to.?cart|ajout.?panier|buy.?now|acheter|btn.?cart|cart.?btn|add.?cart",
    re.I,
)

_ADD_TO_CART_FORM_ACTIONS = re.compile(
    r"/cart/add|/panier/ajouter|add_to_cart|addtocart",
    re.I,
)


def has_add_to_cart_signals(html) -> tuple[bool, list[str]]:
    soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html
    signals = []

    for pattern in _ADD_TO_CART_PATTERNS:
        btn = soup.find(["button", "a", "input"], string=pattern)
        if btn:
            signals.append(f"button_text:{pattern.pattern}")
            break
        btn = soup.find(["button", "a", "input"], attrs={"value": pattern})
        if btn:
            signals.append(f"input_value:{pattern.pattern}")
            break

    for btn in soup.find_all(["button", "a"], class_=True):
        classes = " ".join(btn.get("class", []))
        if _ADD_TO_CART_CLASSES.search(classes):
            signals.append(f"cart_class:{classes[:60]}")
            break

    for form in soup.find_all("form", action=True):
        action = form.get("action", "")
        if _ADD_TO_CART_FORM_ACTIONS.search(action):
            signals.append(f"form_action:{action[:80]}")
            break

    return bool(signals), signals


def has_price_signals(html) -> tuple[bool, list[str]]:
    soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html
    signals = []

    for tag in soup.find_all("meta"):
        prop = (tag.get("property", "") or tag.get("name", "")).lower()
        content = tag.get("content", "")
        if prop in ("product:price:amount", "og:price:amount", "twitter:data1") and content:
            try:
                val = float(content.replace(",", ".").replace(" ", ""))
                if val > 0:
                    signals.append(f"meta:{prop}={content}")
            except (ValueError, TypeError):
                pass

    price_classes = re.compile(
        r"price|prix|product-price|current-price|sale-price|our-price|final-price",
        re.I,
    )
    price_el = soup.find(class_=price_classes)
    if price_el:
        text = price_el.get_text(strip=True)
        if re.search(r"\d+[.,]\d{2}\s*[€$£]|\d+\s*[€$£]|[€$£]\s*\d+", text):
            signals.append(f"price_element:{text[:40]}")

    price_itemprop = soup.find(attrs={"itemprop": "price"})
    if price_itemprop:
        val = price_itemprop.get("content", "") or price_itemprop.get_text(strip=True)
        if val:
            signals.append(f"itemprop_price:{val[:30]}")

    return bool(signals), signals


def has_weight_signals(text: str) -> tuple[bool, list[str]]:
    signals = []
    t = (text or "").lower()

    kg_match = re.search(r"(\d+(?:[.,]\d+)?)\s*kg\b", t)
    if kg_match:
        val = float(kg_match.group(1).replace(",", "."))
        if 0.2 <= val <= 5.0:
            signals.append(f"weight_kg:{val}")

    g_match = re.search(r"(\d+(?:[.,]\d+)?)\s*g(?:r(?:amme)?)?(?:\b|[^a-z])", t)
    if g_match:
        val = float(g_match.group(1).replace(",", "."))
        if 200 <= val <= 5000:
            signals.append(f"weight_g:{val}")

    return bool(signals), signals


def _is_editorial_page(soup: BeautifulSoup, has_jsonld_offer: bool) -> tuple[bool, str]:
    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(strip=True).lower()
        for pattern in EDITORIAL_H1_PATTERNS:
            if re.search(pattern, h1_text):
                if not has_jsonld_offer:
                    return True, f"editorial_h1:{h1_text[:60]}"

    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True).lower()
        for pattern in EDITORIAL_H1_PATTERNS:
            if re.search(pattern, title_text):
                if not has_jsonld_offer:
                    return True, f"editorial_title:{title_text[:60]}"

    return False, ""


def _is_content_heavy_page(soup: BeautifulSoup, has_cart: bool, has_jsonld_offer: bool) -> tuple[bool, str]:
    body = soup.find("body")
    if not body:
        return False, ""

    text = body.get_text(" ", strip=True)
    word_count = len(text.split())

    if word_count > 1200 and not has_cart and not has_jsonld_offer:
        return True, f"content_heavy:word_count={word_count}"

    return False, ""


def is_product_page(url: str, html) -> tuple[bool, dict]:
    reasons = {
        "accepted": False,
        "rejection_reason": "",
        "signals": {},
    }

    bad, bad_reason = is_bad_url(url)
    if bad:
        reasons["rejection_reason"] = bad_reason
        logger.info(f"[PAGE_VALIDATOR] REJECTED (bad_url) {url} => {bad_reason}")
        return False, reasons

    soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html

    jsonld_info = extract_jsonld_product_offer(soup)
    reasons["signals"]["jsonld"] = jsonld_info

    cart_found, cart_signals = has_add_to_cart_signals(soup)
    reasons["signals"]["add_to_cart"] = cart_signals

    price_found, price_signals = has_price_signals(soup)
    reasons["signals"]["price"] = price_signals

    page_text = soup.get_text(" ", strip=True)
    weight_found, weight_signals = has_weight_signals(page_text)
    reasons["signals"]["weight"] = weight_signals

    has_jsonld_offer = jsonld_info["has_product"] and jsonld_info["has_offer"]
    has_jsonld_price = jsonld_info["has_price"]

    is_editorial, editorial_reason = _is_editorial_page(soup, has_jsonld_offer)
    if is_editorial:
        reasons["rejection_reason"] = editorial_reason
        logger.info(f"[PAGE_VALIDATOR] REJECTED (editorial) {url} => {editorial_reason}")
        return False, reasons

    content_heavy, content_reason = _is_content_heavy_page(soup, cart_found, has_jsonld_offer)
    if content_heavy:
        reasons["rejection_reason"] = content_reason
        logger.info(f"[PAGE_VALIDATOR] REJECTED (content_heavy) {url} => {content_reason}")
        return False, reasons

    if has_jsonld_offer and (has_jsonld_price or jsonld_info["has_availability"]):
        reasons["accepted"] = True
        reasons["acceptance_path"] = "jsonld_product_offer"
        logger.debug(f"[PAGE_VALIDATOR] ACCEPTED (jsonld) {url}")
        return True, reasons

    if cart_found and price_found and weight_found:
        reasons["accepted"] = True
        reasons["acceptance_path"] = "cart+price+weight"
        logger.debug(f"[PAGE_VALIDATOR] ACCEPTED (signals) {url}")
        return True, reasons

    if cart_found and price_found:
        reasons["accepted"] = True
        reasons["acceptance_path"] = "cart+price"
        logger.debug(f"[PAGE_VALIDATOR] ACCEPTED (cart+price, no weight) {url}")
        return True, reasons

    parts = []
    if not has_jsonld_offer:
        parts.append("no_jsonld_offer")
    if not cart_found:
        parts.append("no_cart")
    if not price_found:
        parts.append("no_price")
    if not weight_found:
        parts.append("no_weight")
    reasons["rejection_reason"] = "insufficient_signals:" + ",".join(parts)
    logger.info(f"[PAGE_VALIDATOR] REJECTED (insufficient) {url} => {reasons['rejection_reason']}")
    return False, reasons


def validate_url_debug(url: str) -> dict:
    import httpx

    result = {
        "url": url,
        "status": "unknown",
        "is_bad_url": False,
        "bad_url_reason": "",
        "http_status": None,
        "is_product_page": False,
        "reasons": {},
        "error": None,
    }

    bad, bad_reason = is_bad_url(url)
    result["is_bad_url"] = bad
    result["bad_url_reason"] = bad_reason

    if bad:
        result["status"] = "rejected_url"
        return result

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=12.0, follow_redirects=True, verify=False) as client:
            response = client.get(url, headers=headers)
            result["http_status"] = response.status_code

            if response.status_code != 200:
                result["status"] = f"http_error:{response.status_code}"
                return result

            is_product, reasons = is_product_page(url, response.text)
            result["is_product_page"] = is_product
            result["reasons"] = reasons
            result["status"] = "accepted" if is_product else "rejected"

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result
