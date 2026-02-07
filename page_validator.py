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
    "/post/", "/pages/",
    "/wiki/", "/glossaire/", "/lexique/",
    "/collections/", "/product-category/",
    "/categorie-produit/", "/categorie/",
    "/info/", "/fr/info/",
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

ARTICLE_URL_PATTERNS = [
    "/blog/", "/guide/", "/types-de/", "/conseils/",
    "/bienfaits/", "/utilisation/", "/tout-savoir/",
    "/comment-", "/pourquoi-", "/difference-entre",
    "/dossier/", "/article/", "/articles/",
    "/magazine/", "/editorial/", "/wiki/",
]

ARTICLE_H1_KEYWORDS = [
    "bienfaits", "utilisation", "guide", "comment",
    "tout savoir", "conseils", "avantages",
    "comparatif", "difference entre", "différence entre",
    "pourquoi", "choisir", "quel", "quelle",
    "top ", "meilleur", "classement", "selection",
    "sélection", "avis", "test",
]


CATEGORY_URL_PATTERNS = re.compile(
    r"(/store/[^/]*/?$"
    r"|/boutique/[^/]*/?$"
    r"|/c/[^/]+/"
    r"|/\d+-[a-z][a-z-]+/?$"
    r"|/\d+_[a-z]"
    r"|proteines-c-\d+"
    r"|/cat-\d+\.html$"
    r")",
    re.I,
)

HOMEPAGE_PATTERN = re.compile(r"^/?$")


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

    if HOMEPAGE_PATTERN.match(path):
        return True, "homepage"

    cat_match = CATEGORY_URL_PATTERNS.search(path)
    if cat_match:
        return True, f"category_url:{cat_match.group(0)}"

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
    re.compile(r"buy\s+now", re.I),
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


def has_purchase_proof(soup: BeautifulSoup) -> tuple[bool, list[str]]:
    proofs = []

    jsonld_info = extract_jsonld_product_offer(soup)
    if jsonld_info["has_product"] and jsonld_info["has_offer"] and (jsonld_info["has_price"] or jsonld_info["has_availability"]):
        proofs.append("jsonld_product_offer_price")

    for tag in soup.find_all("meta"):
        prop = (tag.get("property", "") or tag.get("name", "")).lower()
        content = tag.get("content", "")
        if prop in ("product:price:amount", "og:price:amount") and content:
            try:
                val = float(content.replace(",", ".").replace(" ", ""))
                if val > 0:
                    proofs.append(f"meta_price:{prop}={content}")
            except (ValueError, TypeError):
                pass

    cart_found, cart_signals = has_add_to_cart_signals(soup)
    price_found, price_signals = has_price_signals(soup)

    if cart_found and price_found:
        proofs.append(f"cart_and_price:{','.join(cart_signals)}|{','.join(price_signals)}")

    return bool(proofs), proofs


def is_article_page(url: str, soup: BeautifulSoup) -> tuple[bool, list[str]]:
    reasons = []
    url_lower = url.lower()
    parsed = urlparse(url_lower)
    path = parsed.path

    for pattern in ARTICLE_URL_PATTERNS:
        if pattern in path:
            reasons.append(f"url_pattern:{pattern}")
            break

    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True).lower() if h1 else ""
    for kw in ARTICLE_H1_KEYWORDS:
        if kw in h1_text:
            reasons.append(f"h1_article_kw:{kw}")
            break

    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True).lower() if title_tag else ""
    for kw in ARTICLE_H1_KEYWORDS:
        if kw in title_text:
            if f"h1_article_kw:{kw}" not in reasons:
                reasons.append(f"title_article_kw:{kw}")
            break

    body = soup.find("body")
    if body:
        text = body.get_text(" ", strip=True)
        word_count = len(text.split())
        has_proof, _ = has_purchase_proof(soup)
        if word_count > 1200 and not has_proof:
            reasons.append(f"content_heavy:word_count={word_count}")

    return bool(reasons), reasons


CATEGORY_TITLE_PATTERNS = [
    re.compile(r"\bachat\s*/\s*vente\b", re.I),
    re.compile(r"\bpas\s+cher\b", re.I),
    re.compile(r"\btoutes?\s+les?\s+(meilleur|whey|protéine|proteine)\b", re.I),
    re.compile(r"\ben\s+ligne\b", re.I),
    re.compile(r"\bdécouvrez\s+tout\b", re.I),
    re.compile(r"\bprotéines?\s+en\s+poudre\s*[|:]", re.I),
    re.compile(r"\bgamme\s+(classique|sans|complète)\b", re.I),
]

CATEGORY_PATH_PATTERNS = [
    "/collections/", "/collections",
    "/product-category/", "/categorie-produit/",
    "/categorie/", "/store/", "/c/",
    "/pages/",
]


def _is_category_page(url: str, soup: BeautifulSoup) -> tuple[bool, list[str]]:
    signals = []
    path = urlparse(url.lower()).path

    for pat in CATEGORY_PATH_PATTERNS:
        if pat in path:
            signals.append(f"cat_url:{pat}")
            break

    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    combined = f"{title_text} {h1_text}"

    for pat in CATEGORY_TITLE_PATTERNS:
        if pat.search(combined):
            signals.append(f"cat_title:{pat.pattern[:40]}")
            break

    product_links = soup.find_all("a", href=re.compile(r"/product[s]?/|/produit/|\.html$", re.I))
    if len(product_links) >= 5:
        signals.append(f"many_product_links:{len(product_links)}")

    product_grids = soup.find_all(class_=re.compile(r"product.?grid|product.?list|product.?card|collection.?grid", re.I))
    if product_grids:
        signals.append(f"product_grid_class")

    return len(signals) >= 2, signals


def _classify_page_type(url: str, soup: BeautifulSoup) -> str:
    has_proof, _ = has_purchase_proof(soup)
    article, _ = is_article_page(url, soup)
    is_cat, _ = _is_category_page(url, soup)

    if has_proof and not article and not is_cat:
        return "product"

    if is_cat:
        return "category"

    if article and not has_proof:
        return "article"

    if has_proof and article:
        return "product"

    return "unknown"


def is_product_page(url: str, html) -> tuple[bool, dict]:
    reasons = {
        "accepted": False,
        "rejection_reason": "",
        "page_type": "unknown",
        "purchase_proof": [],
        "article_signals": [],
        "signals": {},
    }

    bad, bad_reason = is_bad_url(url)
    if bad:
        reasons["rejection_reason"] = bad_reason
        reasons["page_type"] = "blocked"
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

    has_proof, proof_details = has_purchase_proof(soup)
    reasons["purchase_proof"] = proof_details

    article_detected, article_reasons = is_article_page(url, soup)
    reasons["article_signals"] = article_reasons

    is_cat, cat_signals = _is_category_page(url, soup)
    reasons["category_signals"] = cat_signals

    page_type = _classify_page_type(url, soup)
    reasons["page_type"] = page_type

    if article_detected and not has_proof:
        reasons["rejection_reason"] = f"article_page:{','.join(article_reasons)}"
        reasons["page_type"] = "article"
        logger.info(f"[PAGE_VALIDATOR] REJECTED (article) {url} => {reasons['rejection_reason']}")
        return False, reasons

    if is_cat:
        reasons["rejection_reason"] = f"category_page:{','.join(cat_signals)}"
        reasons["page_type"] = "category"
        logger.info(f"[PAGE_VALIDATOR] REJECTED (category) {url} => {reasons['rejection_reason']}")
        return False, reasons

    if not has_proof:
        parts = []
        if not jsonld_info.get("has_offer"):
            parts.append("no_jsonld_offer")
        if not cart_found:
            parts.append("no_cart")
        if not price_found:
            parts.append("no_price")
        reasons["rejection_reason"] = f"no_purchase_proof:{','.join(parts)}"
        reasons["page_type"] = page_type if page_type != "unknown" else "unknown"
        logger.info(f"[PAGE_VALIDATOR] REJECTED (no_purchase_proof) {url} => {reasons['rejection_reason']}")
        return False, reasons

    reasons["accepted"] = True
    reasons["page_type"] = "product"

    if jsonld_info["has_product"] and jsonld_info["has_offer"] and (jsonld_info["has_price"] or jsonld_info["has_availability"]):
        reasons["acceptance_path"] = "jsonld_product_offer"
    elif cart_found and price_found and weight_found:
        reasons["acceptance_path"] = "cart+price+weight"
    elif cart_found and price_found:
        reasons["acceptance_path"] = "cart+price"
    else:
        reasons["acceptance_path"] = "meta_price"

    logger.debug(f"[PAGE_VALIDATOR] ACCEPTED ({reasons['acceptance_path']}) {url}")
    return True, reasons


WHEY_KEYWORDS = [
    "whey", "protéine whey", "proteine whey", "whey protein",
    "whey isolate", "whey native", "whey concentr", "whey hydrolys",
    "isolat de whey", "isolat de lactosérum", "isolat de lactoserum",
    "lactosérum", "lactoserum",
    "protéine de lactosérum", "proteine de lactoserum",
]

NON_WHEY_KEYWORDS = [
    "endurance", "boisson energetique", "boisson énergétique",
    "boisson isotonique", "bcaa seul", "creatine", "créatine",
    "barre protéinée", "barre proteinee", "mass gainer",
    "pre-workout", "pre workout", "bruleur", "brûleur",
    "collagene", "collagène", "vitamines", "omega",
]


def _count_whey_signals(url: str, html, soup=None) -> tuple[int, list[str]]:
    if soup is None:
        soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html

    signals = []
    url_lower = url.lower()

    if any(kw in url_lower for kw in ["whey", "proteine-whey", "protéine-whey"]):
        signals.append(f"url_contains_whey")

    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True).lower() if h1 else ""
    if any(kw in h1_text for kw in WHEY_KEYWORDS):
        signals.append(f"h1_whey:{h1_text[:60]}")

    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True).lower() if title_tag else ""
    if any(kw in title_text for kw in WHEY_KEYWORDS):
        signals.append(f"title_whey:{title_text[:60]}")

    jsonld_info = extract_jsonld_product_offer(soup)
    jsonld_name = (jsonld_info.get("product_name") or "").lower()
    if any(kw in jsonld_name for kw in WHEY_KEYWORDS):
        signals.append(f"jsonld_name_whey:{jsonld_name[:60]}")

    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and item.get("@graph"):
                    items.extend(item["@graph"])
            for item in items:
                if isinstance(item, dict):
                    desc = str(item.get("description", "")).lower()
                    if any(kw in desc for kw in WHEY_KEYWORDS):
                        signals.append("jsonld_desc_whey")
                        break
        except (json.JSONDecodeError, TypeError):
            continue

    breadcrumbs = soup.find_all(["nav", "ol", "ul"], class_=re.compile(r"breadcrumb", re.I))
    for bc in breadcrumbs:
        bc_text = bc.get_text(" ", strip=True).lower()
        if any(kw in bc_text for kw in WHEY_KEYWORDS):
            signals.append(f"breadcrumb_whey:{bc_text[:60]}")
            break

    for a in soup.find_all("a", attrs={"itemprop": "item"}):
        a_text = a.get_text(strip=True).lower()
        if any(kw in a_text for kw in WHEY_KEYWORDS):
            signals.append(f"breadcrumb_link_whey:{a_text[:40]}")
            break

    meta_desc = ""
    for tag in soup.find_all("meta"):
        prop = (tag.get("property", "") or tag.get("name", "")).lower()
        if prop in ("description", "og:description"):
            meta_desc = (tag.get("content", "") or "").lower()
            break
    if meta_desc and any(kw in meta_desc for kw in WHEY_KEYWORDS):
        signals.append("meta_desc_whey")

    return len(signals), signals


def _has_non_whey_signals(url: str, html, soup=None) -> tuple[bool, list[str]]:
    if soup is None:
        soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html

    signals = []
    url_lower = url.lower()
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True).lower() if h1 else ""
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True).lower() if title_tag else ""

    combined = f"{url_lower} {h1_text} {title_text}"

    for kw in NON_WHEY_KEYWORDS:
        if kw in combined and "whey" not in combined:
            signals.append(f"non_whey_keyword:{kw}")

    return bool(signals), signals


def is_whey_product_page(html, url: str) -> tuple[bool, dict]:
    result = {
        "is_product": False,
        "is_whey": False,
        "page_type": "unknown",
        "whey_signal_count": 0,
        "whey_signals": [],
        "non_whey_signals": [],
        "purchase_proof": [],
        "article_signals": [],
        "product_reasons": {},
        "rejection_reason": "",
    }

    soup = BeautifulSoup(html, "lxml") if isinstance(html, str) else html

    is_prod, prod_reasons = is_product_page(url, soup)
    result["is_product"] = is_prod
    result["product_reasons"] = prod_reasons
    result["page_type"] = prod_reasons.get("page_type", "unknown")
    result["purchase_proof"] = prod_reasons.get("purchase_proof", [])
    result["article_signals"] = prod_reasons.get("article_signals", [])

    if not is_prod:
        result["rejection_reason"] = f"not_product_page:{prod_reasons.get('rejection_reason', '')}"
        logger.info(f"[WHEY_VALIDATOR] REJECTED (not product, type={result['page_type']}) {url}")
        return False, result

    is_non_whey, non_whey_signals = _has_non_whey_signals(url, soup, soup)
    result["non_whey_signals"] = non_whey_signals
    if is_non_whey:
        result["rejection_reason"] = f"non_whey_product:{', '.join(non_whey_signals)}"
        logger.info(f"[WHEY_VALIDATOR] REJECTED (non-whey) {url} => {non_whey_signals}")
        return False, result

    count, whey_signals = _count_whey_signals(url, soup, soup)
    result["whey_signal_count"] = count
    result["whey_signals"] = whey_signals

    if count >= 2:
        result["is_whey"] = True
        result["page_type"] = "product"
        logger.debug(f"[WHEY_VALIDATOR] ACCEPTED {url} ({count} whey signals: {whey_signals})")
        return True, result

    result["rejection_reason"] = f"insufficient_whey_signals:{count} (need 2+)"
    logger.info(f"[WHEY_VALIDATOR] REJECTED (not enough whey signals) {url} => {count} signals: {whey_signals}")
    return False, result


def validate_url_debug(url: str) -> dict:
    import httpx

    result = {
        "url": url,
        "status": "unknown",
        "page_type": "unknown",
        "is_bad_url": False,
        "bad_url_reason": "",
        "http_status": None,
        "is_product_page": False,
        "has_purchase_proof": False,
        "purchase_proof": [],
        "is_article": False,
        "article_signals": [],
        "reasons": {},
        "error": None,
    }

    bad, bad_reason = is_bad_url(url)
    result["is_bad_url"] = bad
    result["bad_url_reason"] = bad_reason

    if bad:
        result["status"] = "rejected_url"
        result["page_type"] = "blocked"
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

            soup = BeautifulSoup(response.text, "lxml")

            has_proof, proof_details = has_purchase_proof(soup)
            result["has_purchase_proof"] = has_proof
            result["purchase_proof"] = proof_details

            article_detected, article_reasons = is_article_page(url, soup)
            result["is_article"] = article_detected
            result["article_signals"] = article_reasons

            is_product, reasons = is_product_page(url, soup)
            result["is_product_page"] = is_product
            result["reasons"] = reasons
            result["page_type"] = reasons.get("page_type", "unknown")
            result["status"] = "accepted" if is_product else "rejected"

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result
