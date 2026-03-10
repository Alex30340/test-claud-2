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
    calculate_protein_score,
    calculate_health_score,
    calculate_global_score,
    calculate_final_score_10,
)
from extractor import extract_price, extract_currency, extract_weight_kg, detect_needs_js_render
from validator import validate_price, validate_weight, validate_price_per_kg, compute_confidence_v2
from page_validator import is_bad_url, is_product_page as strict_is_product_page

logger = logging.getLogger(__name__)


class BraveAPIError(Exception):
    pass

SEARCH_QUERIES = [
    "acheter whey protein 1kg site:*.fr",
    "whey isolate achat boutique en ligne",
    "whey hydrolysate acheter fiche produit",
    "whey native francaise acheter",
    "impact whey protein myprotein achat",
    "clear whey isolate acheter",
    "optimum nutrition gold standard whey acheter",
    "bulk powders whey protein acheter",
    "scitec nutrition whey protein acheter",
    "eric favre whey protein boutique",
    "foodspring whey protein acheter",
    "whey isolate sans sucralose boutique",
    "whey proteine fabrication francaise acheter",
    "nutrimuscle whey native acheter",
    "protealpes whey francaise boutique",
    "alter nutrition whey acheter",
]

SEED_BRANDS = {
    "Novoma": "novoma.com",
    "Nutrimuscle": "nutrimuscle.com",
    "Nutri&Co": "nutriandco.com",
    "Eiyolab": "eiyolab.com",
    "Greenwhey": "greenwhey.fr",
    "Nutripure": "nutripure.fr",
    "Foodspring": "foodspring.fr",
    "Optigura": "optigura.fr",
    "Myprotein": "myprotein.fr",
    "Bulk": "bulk.com",
    "ESN": "esn.com",
    "Eric Favre": "ericfavre.com",
    "Protealpes": "protealpes.com",
    "Alter Nutrition": "alternutrition.com",
    "The Protein Works": "fr.theproteinworks.com",
    "Scitec Nutrition": "scitecnutrition.com",
    "Optimum Nutrition": None,
    "Biotech USA": "biotechusa.fr",
    "QNT": "qnt.com",
    "Olimp": "olimp.fr",
    "Nu3": "nu3.fr",
    "Prozis": "prozis.com",
    "Harder": "harder.fr",
    "EAFIT": "eafit.com",
    "Apurna": None,
    "Dymatize": None,
    "Iron Factory": None,
    "Impact Nutrition": None,
}

BLOCK_DOMAINS = [
    "myprotein.fr", "myprotein.com",
    "bulk.com",
    "amazon.fr", "amazon.com",
    "decathlon.fr",
]

MAX_PER_DOMAIN = 2

WHEY_TYPE_QUERIES = ["whey isolate", "whey native", "whey concentree", "whey hydrolysee"]

INTENT_KEYWORDS = ['"ajouter au panier"', '"en stock"', '"acheter"', '"prix"']

SEARCH_EXCLUSIONS = "-blog -forum -comparatif -guide -test -avis -pdf -category -collections -search"


def generate_discovery_queries(use_brand_seeds: bool = True, block_domains: list[str] | None = None) -> list[dict]:
    queries = []
    block = block_domains or []
    block_str = " ".join(f"-site:{d}" for d in block) if block else ""

    for wtype in WHEY_TYPE_QUERIES:
        for intent in INTENT_KEYWORDS:
            q = f'{wtype} {intent} site:.fr {SEARCH_EXCLUSIONS}'
            if block_str:
                q += f" {block_str}"
            queries.append({"query": q, "source": f"type:{wtype}"})

    if use_brand_seeds:
        for brand, domain in SEED_BRANDS.items():
            q = f'whey "{brand}" acheter site:.fr {SEARCH_EXCLUSIONS}'
            queries.append({"query": q, "source": f"brand:{brand}"})

            if domain:
                q2 = f'site:{domain} whey'
                queries.append({"query": q2, "source": f"brand_site:{brand}"})

    longtail_queries = [
        f'whey isolate "€" "ajouter au panier" site:.fr {block_str}',
        f'whey native francaise "acheter" site:.fr {block_str}',
        f'proteine whey "en stock" "1kg" site:.fr {SEARCH_EXCLUSIONS} {block_str}',
        f'whey isolate "2kg" acheter site:.fr {SEARCH_EXCLUSIONS} {block_str}',
        f'clear whey isolate acheter site:.fr {SEARCH_EXCLUSIONS} {block_str}',
        f'whey sans sucralose acheter site:.fr {SEARCH_EXCLUSIONS} {block_str}',
    ]
    for q in longtail_queries:
        queries.append({"query": q.strip(), "source": "longtail"})

    for q_text in SEARCH_QUERIES:
        queries.append({"query": q_text, "source": "legacy"})

    return queries

EXCLUDED_DOMAINS = [
    "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "reddit.com", "forum", "blog", "wikipedia.org",
    "pinterest.com", "linkedin.com", "quora.com",
    "amazon.fr", "amazon.com",
    "idealo.fr", "idealo.com",
    "decathlon.fr", "decathlon.com",
    "doctissimo.fr", "passeportsante.net", "sante.journaldesfemmes.fr",
    "marmiton.org", "femmeactuelle.fr", "20minutes.fr", "lequipe.fr",
    "lefigaro.fr", "lemonde.fr", "bfmtv.com",
]

EXCLUDED_PATH_KEYWORDS = [
    "/blog/", "/forum/", "/article/", "/comparatif/", "/avis/", "/guide/",
    "/video/", "/news/", "/actualite/", "/conseil/", "/faq/", "/aide/",
    "/recette/", "/tutoriel/", "/dossier/", "/magazine/", "/editorial/",
    "/top-", "/classement/", "/versus/", "/vs-",
    "/comment-choisir", "/pourquoi-", "/difference-entre",
    "/post/", "/category/", "/tag/",
]

REQUEST_DELAY = 1.1
HTTP_TIMEOUT = 12.0
MAX_WORKERS = 8

SWEETENERS = {
    "sucralose": ["sucralose", "splenda"],
    "acesulfame_k": ["acesulfame", "acésulfame", "acesulfame-k", "e950"],
    "aspartame": ["aspartame", "e951"],
}

WHEY_TYPES_NAME_PRIORITY = [
    ("native", ["whey native", "native whey", "native"]),
    ("hydrolysate", ["hydrolys", "hydrolyzed", "hydrolysee", "hydrolysée", "hydrolysat"]),
    ("isolate", ["isolat", "isolate", "iso whey", "whey iso"]),
    ("concentrate", ["concentrate", "concentre", "concentré", "concentrat"]),
]

WHEY_TYPES = {k: v for k, v in WHEY_TYPES_NAME_PRIORITY}

ORIGIN_FR_PATTERNS = [
    r"fabriqu[ée]e?\s+en\s+france",
    r"made\s+in\s+france",
    r"origine\s+france",
    r"lait\s+d['']\s*origine\s+fran[cç]aise?",
    r"lait\s+fran[cç]ais",
    r"produit\s+en\s+france",
]

KNOWN_BRANDS = [
    "myprotein", "optimum nutrition", "bulk", "bulk powders", "scitec nutrition",
    "eric favre", "foodspring", "nutrimuscle", "eafit", "apurna", "biotech usa",
    "dymatize", "bsn", "muscletech", "nutri&co", "protealpes", "alter nutrition",
    "pure whey", "qnt", "olimp", "nu3", "prozis", "impact nutrition",
    "the protein works", "warriors", "iron factory", "harder", "gold standard",
    "iso whey", "native whey", "whey zero",
]


PRODUCT_PATH_SIGNALS = [
    "/produit/", "/product/", "/shop/", "/boutique/",
    "/achat/", "/acheter/", "/proteines/",
    "/whey", "/isolate", "/proteine",
]

NON_PRODUCT_TITLE_KEYWORDS = [
    "comparatif", "guide", "avis", "top 10", "top 5", "meilleur",
    "comment choisir", "quelle whey", "difference entre",
    "classement", "versus", "vs ", "test et avis",
    "notre selection", "selection des", "les meilleures",
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


def is_product_page(soup: BeautifulSoup, url: str) -> bool:
    add_to_cart_patterns = [
        re.compile(r"ajout.*panier", re.I),
        re.compile(r"add.*cart", re.I),
        re.compile(r"ajouter.*panier", re.I),
        re.compile(r"acheter", re.I),
        re.compile(r"commander", re.I),
    ]
    for pattern in add_to_cart_patterns:
        btn = soup.find(["button", "a", "input"], string=pattern)
        if btn:
            return True
        btn = soup.find(["button", "a", "input"], attrs={"value": pattern})
        if btn:
            return True
        btn = soup.find(["button", "a"], class_=re.compile(r"add.?to.?cart|ajout.?panier|buy|acheter", re.I))
        if btn:
            return True

    og = extract_og_meta(soup)
    if og.get("og:type", "").lower() in ("product", "og:product", "product.item"):
        return True

    price_el = soup.find(class_=re.compile(r"price|prix|product-price|current-price", re.I))
    product_el = soup.find(class_=re.compile(r"product-detail|product-info|product-page|fiche-produit", re.I))
    if price_el and product_el:
        return True

    path = urlparse(url).path.lower()
    has_product_path = any(sig in path for sig in PRODUCT_PATH_SIGNALS)
    if has_product_path and price_el:
        return True

    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True).lower()
        for kw in NON_PRODUCT_TITLE_KEYWORDS:
            if kw in title_text:
                return False

    return False


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
            if response.status_code == 429:
                logger.warning(f"Rate limited on query: {query}, waiting 5s...")
                time.sleep(5)
                response = client.get(url, headers=headers, params=params)
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
        val = float(match.group(1))
        if 0.1 <= val <= 25:
            return val

    match = re.search(r"(\d+(?:\.\d+)?)\s*g(?:r|ramme)?(?:\b|[^a-z])", text)
    if match:
        grams = float(match.group(1))
        if 100 <= grams <= 25000:
            return grams / 1000

    return None


WHEY_PRICE_MIN = 8.0
WHEY_PRICE_MAX = 200.0


def parse_price(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if WHEY_PRICE_MIN <= v <= WHEY_PRICE_MAX:
            return v
        return None
    text = str(value).replace(",", ".").replace("\u00a0", "").replace(" ", "").strip()
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        v = float(match.group(1))
        if WHEY_PRICE_MIN <= v <= WHEY_PRICE_MAX:
            return v
    return None


def extract_best_price(soup: BeautifulSoup, jsonld: dict | None, og: dict, microdata: dict) -> float | None:
    prices = []

    if jsonld:
        offers = jsonld.get("offers", {})
        if isinstance(offers, list):
            for offer in offers:
                p = parse_price(offer.get("price"))
                if p:
                    prices.append(("jsonld", p))
        elif isinstance(offers, dict):
            p = parse_price(offers.get("price"))
            if p:
                prices.append(("jsonld", p))
            low = parse_price(offers.get("lowPrice"))
            if low:
                prices.append(("jsonld_low", low))

    og_price = parse_price(og.get("product:price:amount", ""))
    if og_price:
        prices.append(("og", og_price))

    micro_price = parse_price(microdata.get("price", ""))
    if micro_price:
        prices.append(("microdata", micro_price))

    old_price_classes = [
        "old-price", "regular-price", "price--old", "price-old",
        "was-price", "price-was", "price--crossed", "prix-barre",
        "list-price", "compare-price",
    ]
    old_price_values = set()
    for cls in old_price_classes:
        el = soup.find(class_=re.compile(re.escape(cls), re.I))
        if el:
            p = parse_price(el.get_text())
            if p:
                old_price_values.add(p)

    priority_classes = [
        "current-price", "sale-price", "product-price", "price--current",
        "price-new", "our-price", "final-price",
    ]
    for cls in priority_classes:
        el = soup.find(class_=re.compile(re.escape(cls), re.I))
        if el:
            p = parse_price(el.get_text())
            if p and p not in old_price_values:
                prices.append(("html_priority", p))
                break

    if not prices:
        for cls in ["price", "prix"]:
            el = soup.find(class_=re.compile(cls, re.I))
            if el:
                p = parse_price(el.get_text())
                if p and p not in old_price_values:
                    prices.append(("html_generic", p))
                    break

    if not prices:
        return None

    source_priority = {"jsonld": 0, "jsonld_low": 1, "og": 2, "microdata": 3, "html_priority": 4, "html_generic": 5}
    prices.sort(key=lambda x: source_priority.get(x[0], 99))

    best_price = prices[0][1]

    return round(best_price, 2)


def parse_protein(text: str) -> float | None:
    if not text:
        return None
    text = text.lower().replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)\s*g?", text)
    if match:
        val = float(match.group(1))
        if 10 < val <= 100:
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


_INGREDIENT_HEADING_PATTERNS = [
    re.compile(r"ingr[ée]dients?\s+du\s+produit", re.I),
    re.compile(r"liste\s+des\s+ingr[ée]dients?", re.I),
    re.compile(r"ingr[ée]dients?", re.I),
    re.compile(r"composition\s+nutritionnelle", re.I),
    re.compile(r"composition\s+du\s+produit", re.I),
    re.compile(r"composition", re.I),
    re.compile(r"what['']?s\s+in\s+it", re.I),
]

_INGREDIENT_NOISE = re.compile(
    r"(menu|footer|header|sidebar|nav|cookie|banner|newsletter|panier|cart|checkout)",
    re.I,
)


def _is_noise_element(el) -> bool:
    if not el:
        return False
    classes = " ".join(el.get("class", []) if hasattr(el, "get") else [])
    el_id = el.get("id", "") if hasattr(el, "get") else ""
    return bool(_INGREDIENT_NOISE.search(f"{classes} {el_id}"))


def find_ingredients_block_html(soup) -> str | None:
    for pattern in _INGREDIENT_HEADING_PATTERNS:
        headings = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b", "p", "span", "div"],
                                  string=pattern)
        for heading in headings:
            if _is_noise_element(heading) or _is_noise_element(heading.parent):
                continue

            sibling_texts = []
            for sib in heading.find_next_siblings():
                if sib.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
                    break
                if _is_noise_element(sib):
                    continue

                if sib.name in ("ul", "ol"):
                    items = [li.get_text(strip=True) for li in sib.find_all("li")]
                    sibling_texts.append(", ".join(items))
                elif sib.name == "table":
                    cells = [td.get_text(strip=True) for td in sib.find_all(["td", "th"])]
                    sibling_texts.append(", ".join(cells))
                else:
                    txt = sib.get_text(" ", strip=True)
                    if txt:
                        sibling_texts.append(txt)

                if sum(len(t) for t in sibling_texts) > 800:
                    break

            if sibling_texts:
                block = " ".join(sibling_texts)
                if len(block) >= 20:
                    return block[:1200]

            parent = heading.parent
            if parent and parent.name in ("div", "section", "article"):
                parent_text = parent.get_text(" ", strip=True)
                heading_text = heading.get_text(strip=True)
                idx = parent_text.find(heading_text)
                if idx >= 0:
                    after = parent_text[idx + len(heading_text):].strip()
                    if len(after) >= 20:
                        return after[:1200]

    for el in soup.find_all(["div", "section", "p", "span"],
                             class_=re.compile(r"ingr[ée]dient|composition|product-ingredients", re.I)):
        if _is_noise_element(el):
            continue
        items = el.find_all("li")
        if items:
            block = ", ".join(li.get_text(strip=True) for li in items)
            if len(block) >= 20:
                return block[:1200]
        text = el.get_text(" ", strip=True)
        if len(text) >= 30:
            return text[:1200]

    for el in soup.find_all(attrs={"itemprop": re.compile(r"ingredient", re.I)}):
        text = el.get_text(" ", strip=True)
        if len(text) >= 20:
            return text[:1200]

    return None


def find_ingredients_block(text: str, soup=None) -> str | None:
    if soup is not None:
        html_result = find_ingredients_block_html(soup)
        if html_result:
            return html_result

    if not text:
        return None
    t = " ".join(text.split())

    patterns = [
        re.compile(r"(?:liste\s+des\s+)?ingr[ée]dients?\s+du\s+produit\s*[:\-–]\s*(.{30,1200})", re.I),
        re.compile(r"(?:liste\s+des\s+)?ingr[ée]dients?\s*[:\-–]\s*(.{30,1200})", re.I),
        re.compile(r"composition\s+(?:du\s+produit|nutritionnelle)\s*[:\-–]\s*(.{30,1200})", re.I),
        re.compile(r"composition\s*[:\-–]\s*(.{30,1200})", re.I),
    ]
    for pat in patterns:
        m = pat.search(t)
        if m:
            return m.group(1)[:1200]

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


def detect_whey_type(text: str, product_name: str = "") -> str:
    name_lower = (product_name or "").lower()
    whey_context = any(kw in name_lower for kw in ["whey", "protéine", "proteine", "protein", "poudre", "isolat", "lactosérum", "lactoserum"])
    if name_lower and whey_context:
        for wtype, variants in WHEY_TYPES_NAME_PRIORITY:
            if any(v in name_lower for v in variants):
                return wtype

    t = (text or "").lower()
    if not name_lower:
        for wtype, variants in WHEY_TYPES_NAME_PRIORITY:
            if any(v in t for v in variants):
                return wtype
    else:
        for wtype, variants in WHEY_TYPES_NAME_PRIORITY:
            count = sum(1 for v in variants if v in t)
            if count >= 2:
                return wtype
            if count == 1:
                for v in variants:
                    if v in t:
                        idx = t.find(v)
                        context = t[max(0, idx - 30):idx + len(v) + 30]
                        if any(kw in context for kw in ["whey", "protéine", "proteine", "protein", "poudre"]):
                            return wtype
    return "unknown"


ORIGIN_EU_PATTERNS = [
    r"origine\s+(?:union\s+)?europ[ée]enne",
    r"origine\s+ue\b",
    r"fabriqu[ée]e?\s+(?:en|dans\s+l[''])\s*(?:union\s+)?europ",
    r"made\s+in\s+(?:eu|europe)\b",
    r"lait\s+(?:d[''])?origine\s+(?:union\s+)?europ",
    r"produit\s+(?:en|dans\s+l[''])\s*(?:union\s+)?europ",
]


def detect_made_in_france(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t, re.I) for p in ORIGIN_FR_PATTERNS)


def extract_origin_label(text: str, made_in_france: bool) -> dict:
    if made_in_france:
        return {"origin_label": "France", "origin_confidence": 0.9}

    t = (text or "").lower()
    if any(re.search(p, t, re.I) for p in ORIGIN_EU_PATTERNS):
        return {"origin_label": "EU", "origin_confidence": 0.7}

    return {"origin_label": "Inconnu", "origin_confidence": 0.3}


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


def _compute_bcaa_per_100g_prot(leucine_g, isoleucine_g, valine_g, protein_per_100g, amino_base):
    if not leucine_g or not isoleucine_g or not valine_g:
        return None
    total_bcaa = leucine_g + isoleucine_g + valine_g
    if amino_base == "per_100g_protein":
        return round(total_bcaa, 1)
    elif amino_base == "per_100g":
        if protein_per_100g and protein_per_100g > 0:
            return round(total_bcaa * 100 / protein_per_100g, 1)
        return None
    elif amino_base == "per_serving":
        return None
    else:
        if total_bcaa > 15 and total_bcaa < 40:
            return round(total_bcaa, 1)
        elif protein_per_100g and protein_per_100g > 0 and total_bcaa < 30:
            computed = round(total_bcaa * 100 / protein_per_100g, 1)
            if 15 <= computed <= 35:
                return computed
            elif 15 <= total_bcaa <= 35:
                return round(total_bcaa, 1)
        return None


ARTIFICIAL_FLAVOR_PATTERNS = [
    r"ar[oô]mes?\s+artificiel",
    r"ar[oô]mes?\s+synth[ée]tique",
    r"ar[oô]me\s+identique\s+au\s+naturel",
    r"artificial\s+flavo",
]

THICKENER_PATTERNS = [
    r"gomme\s+(?:de\s+)?xanthane",
    r"xanthan\s+gum",
    r"carraghe?[ée]nane?s?",
    r"carrageenan",
    r"gomme\s+(?:de\s+)?guar",
    r"guar\s+gum",
    r"gomme\s+(?:de\s+)?cellulose",
    r"[ée]paississant",
    r"e407\b", r"e415\b", r"e412\b",
    r"gomme\s+(?:d[''])?acacia",
    r"e414\b",
]

COLORANT_PATTERNS = [
    r"colorant",
    r"e1[0-9]{2}\b",
    r"dioxyde\s+de\s+titane",
    r"e171\b",
    r"beta[\s-]?carot[eè]ne",
    r"caramel\s+color",
]


def detect_artificial_flavors(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t) for p in ARTIFICIAL_FLAVOR_PATTERNS)


def detect_thickeners(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t) for p in THICKENER_PATTERNS)


def detect_colorants(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t) for p in COLORANT_PATTERNS)


def count_ingredients(ingredients_text: str | None) -> int | None:
    if not ingredients_text:
        return None
    cleaned = re.sub(r"\(.*?\)", "", ingredients_text)
    parts = re.split(r"[,;]", cleaned)
    parts = [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]
    return len(parts) if parts else None


def extract_amino_values(text: str, protein_per_100g: float | None) -> dict:
    result = {
        "bcaa_per_100g_prot": None,
        "leucine_g": None,
        "isoleucine_g": None,
        "valine_g": None,
    }

    if not text:
        return result

    t = (text or "").lower()

    leucine_patterns = [
        r"l[\-\s]?leucine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
        r"leucine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
        r"leucine\s*[:\(\|]*\s*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
    ]
    isoleucine_patterns = [
        r"l[\-\s]?isoleucine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
        r"isoleucine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
    ]
    valine_patterns = [
        r"l[\-\s]?valine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
        r"valine\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
    ]
    bcaa_patterns = [
        r"bcaa\s*(?:total|totaux)?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(?:mg|g)",
        r"(\d+(?:[.,]\d+)?)\s*(?:mg|g)\s*(?:de\s+)?bcaa",
    ]

    def find_value(patterns, text):
        for p in patterns:
            m = re.search(p, text)
            if m:
                val = float(m.group(1).replace(",", "."))
                unit_match = re.search(p, text)
                full_match = unit_match.group(0) if unit_match else ""
                if "mg" in full_match:
                    val = val / 1000
                if 0.1 <= val <= 50:
                    return val
        return None

    result["leucine_g"] = find_value(leucine_patterns, t)
    result["isoleucine_g"] = find_value(isoleucine_patterns, t)
    result["valine_g"] = find_value(valine_patterns, t)

    bcaa_direct = find_value(bcaa_patterns, t)
    if bcaa_direct:
        if protein_per_100g and protein_per_100g > 0:
            result["bcaa_per_100g_prot"] = round((bcaa_direct / protein_per_100g) * 100, 1)
    elif result["leucine_g"] and result["isoleucine_g"] and result["valine_g"]:
        bcaa_total = result["leucine_g"] + result["isoleucine_g"] + result["valine_g"]
        if protein_per_100g and protein_per_100g > 0:
            result["bcaa_per_100g_prot"] = round((bcaa_total / protein_per_100g) * 100, 1)

    if protein_per_100g and protein_per_100g > 0:
        if result["leucine_g"]:
            result["leucine_g"] = round((result["leucine_g"] / protein_per_100g) * 100, 1)
        if result["isoleucine_g"]:
            result["isoleucine_g"] = round((result["isoleucine_g"] / protein_per_100g) * 100, 1)
        if result["valine_g"]:
            result["valine_g"] = round((result["valine_g"] / protein_per_100g) * 100, 1)

    return result


def extract_brand_from_text(name: str, url: str) -> str:
    combined = (name + " " + url).lower()
    for brand in KNOWN_BRANDS:
        if brand in combined:
            return brand.title()

    domain = urlparse(url).netloc.lower().replace("www.", "")
    parts = domain.split(".")
    if parts and parts[0] not in ("shop", "store", "boutique", "pro"):
        return parts[0].title()
    return ""


def extract_nutrition_from_table(soup: BeautifulSoup) -> dict:
    result = {"protein": None, "calories": None, "fat": None, "carbs": None, "sugar": None, "fiber": None, "salt": None}

    page_text = soup.get_text(" ", strip=True).lower()

    protein_patterns = [
        r"prot[ée]ines?\s*(?:par\s+100\s*g)?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g",
        r"(\d+(?:[.,]\d+)?)\s*g\s*(?:de\s+)?prot[ée]ines?\s*(?:par|pour|/)\s*100\s*g",
        r"prot[ée]ines?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*(?:g|%)",
        r"protein\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g",
        r"(\d+(?:[.,]\d+)?)\s*g\s*(?:de\s+)?prot[ée]ines?",
        r"(\d+(?:[.,]\d+)?)\s*%\s*(?:de\s+)?prot[ée]ines?",
        r"teneur\s+en\s+prot[ée]ines?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*(?:g|%)",
        r"pour\s+100\s*g.*?prot[ée]ines?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)",
        r"prot[ée]ines?\s*\(?\s*pour\s+100\s*g\s*\)?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)",
    ]

    for pattern in protein_patterns:
        match = re.search(pattern, page_text)
        if match:
            val = float(match.group(1).replace(",", "."))
            if 10 < val <= 100:
                result["protein"] = val
                break

    if not result["protein"]:
        tables = soup.find_all("table")
        for table in tables:
            table_text = table.get_text(" ", strip=True).lower()
            if "prot" in table_text or "nutritio" in table_text or "valeur" in table_text:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        if "prot" in label:
                            val_text = cells[1].get_text(strip=True)
                            v = parse_protein(val_text)
                            if v:
                                result["protein"] = v
                                break
                        if len(cells) >= 3 and "prot" in label:
                            val_text = cells[1].get_text(strip=True)
                            v = parse_protein(val_text)
                            if not v:
                                val_text = cells[2].get_text(strip=True)
                                v = parse_protein(val_text)
                            if v:
                                result["protein"] = v
                                break

    if not result["protein"]:
        dl_tags = soup.find_all("dl")
        for dl in dl_tags:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                if "prot" in dt.get_text(strip=True).lower():
                    v = parse_protein(dd.get_text(strip=True))
                    if v:
                        result["protein"] = v
                        break

    if not result["protein"]:
        divs = soup.find_all(["div", "span", "li", "p"], class_=re.compile(r"nutri|protein|valeur|info", re.I))
        for div in divs:
            text = div.get_text(" ", strip=True).lower()
            if "prot" in text:
                match = re.search(r"(\d+(?:[.,]\d+)?)\s*g", text)
                if match:
                    v = float(match.group(1).replace(",", "."))
                    if 10 < v <= 100:
                        result["protein"] = v
                        break

    if not result["protein"]:
        section_match = re.search(
            r"(?:valeurs?\s+nutritionn|informations?\s+nutritionn|nutrition|pour\s+100\s*g).*?prot[ée]ines?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*g",
            page_text,
        )
        if section_match:
            v = float(section_match.group(1).replace(",", "."))
            if 10 < v <= 100:
                result["protein"] = v

    if not result["protein"]:
        section_match = re.search(
            r"par\s+(?:dose|portion|serving|scoop).*?prot[ée]ines?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*g",
            page_text,
        )
        if section_match:
            v = float(section_match.group(1).replace(",", "."))
            if 5 < v <= 60:
                dose_size_match = re.search(r"(?:dose|portion|serving|scoop)\s*(?:de)?\s*(\d+)\s*g", page_text)
                if dose_size_match:
                    dose_g = float(dose_size_match.group(1))
                    if 20 <= dose_g <= 50:
                        per_100 = round((v / dose_g) * 100, 1)
                        if 10 < per_100 <= 100:
                            result["protein"] = per_100

    return result


def extract_weight_comprehensive(soup: BeautifulSoup, name: str, jsonld: dict | None) -> float | None:
    combined_text = name.lower().replace(",", ".")

    if jsonld:
        desc = jsonld.get("description", "")
        if isinstance(desc, str):
            combined_text += " " + desc.lower().replace(",", ".")

    weight = parse_weight(combined_text)
    if weight:
        return weight

    if jsonld:
        weight_data = jsonld.get("weight", "")
        if isinstance(weight_data, dict):
            weight_data = weight_data.get("value", "")
        weight = parse_weight(str(weight_data))
        if weight:
            return weight

    title_tag = soup.find("title")
    if title_tag:
        weight = parse_weight(title_tag.get_text(strip=True))
        if weight:
            return weight

    h1 = soup.find("h1")
    if h1:
        weight = parse_weight(h1.get_text(strip=True))
        if weight:
            return weight

    weight_elements = soup.find_all(["span", "div", "p", "li"], class_=re.compile(r"weight|poids|size|taille|format", re.I))
    for el in weight_elements:
        weight = parse_weight(el.get_text(strip=True))
        if weight:
            return weight

    breadcrumbs = soup.find_all(class_=re.compile(r"breadcrumb", re.I))
    for bc in breadcrumbs:
        weight = parse_weight(bc.get_text(strip=True))
        if weight:
            return weight

    page_text = soup.get_text(" ", strip=True).lower().replace(",", ".")
    weight_patterns = [
        r"(?:poids|contenance|format|taille|net\s*wt|net\s+weight)\s*[:\s]*(\d+(?:\.\d+)?)\s*kg",
        r"(?:poids|contenance|format|taille|net\s*wt|net\s+weight)\s*[:\s]*(\d{3,5})\s*g(?:r|ramme)?",
        r"(\d+(?:\.\d+)?)\s*kg\s*(?:de\s+)?(?:whey|prot[ée]ine|poudre)",
        r"(?:whey|prot[ée]ine|poudre)\s*(?:de\s+)?(\d+(?:\.\d+)?)\s*kg",
    ]

    for pattern in weight_patterns:
        match = re.search(pattern, page_text)
        if match:
            groups = match.groups()
            val_str = groups[0]
            val = float(val_str)
            if "kg" in pattern or val < 100:
                if 0.1 <= val <= 25:
                    return val
            else:
                if 100 <= val <= 25000:
                    return val / 1000

    general_kg = re.findall(r"(\d+(?:\.\d+)?)\s*kg", page_text)
    for v_str in general_kg:
        v = float(v_str)
        if 0.2 <= v <= 12:
            return v

    general_g = re.findall(r"(\d{3,5})\s*g(?:r|ramme)?", page_text)
    for v_str in general_g:
        v = float(v_str)
        if 200 <= v <= 12000:
            return v / 1000

    return None


_IMAGE_EXCLUDE_PATTERNS = [
    "favicon", "logo", "icon", "sprite", "placeholder", "loading",
    "banner", "badge", "flag", "payment", "social", "arrow", "close",
    "1x1", "pixel", "spacer", "blank", "tracking",
]

def _extract_product_image_fallback(soup) -> str:
    meta_img = soup.find("meta", {"property": "product:image"})
    if meta_img and meta_img.get("content"):
        return meta_img["content"]
    link_img = soup.find("link", {"rel": "image_src"})
    if link_img and link_img.get("href"):
        return link_img["href"]

    selectors = [
        "[data-product-image] img",
        ".product-image img",
        ".product-gallery img",
        ".product-photo img",
        ".product__media img",
        ".product-single__photo img",
        "[class*='product'] [class*='image'] img",
        "[class*='product'] [class*='gallery'] img",
        ".woocommerce-product-gallery img",
        "[itemprop='image']",
    ]
    for sel in selectors:
        imgs = soup.select(sel)
        for img in imgs:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            if not src:
                continue
            src_lower = src.lower()
            if any(p in src_lower for p in _IMAGE_EXCLUDE_PATTERNS):
                continue
            if src.endswith(".svg") or "data:image" in src:
                continue
            return src

    main_content = soup.find("main") or soup.find("div", {"id": re.compile(r"product|content", re.I)}) or soup
    imgs = main_content.find_all("img", src=True, limit=20)
    for img in imgs:
        src = img.get("src", "")
        src_lower = src.lower()
        if any(p in src_lower for p in _IMAGE_EXCLUDE_PATTERNS):
            continue
        if src.endswith(".svg") or "data:image" in src:
            continue
        width = img.get("width", "")
        height = img.get("height", "")
        try:
            w = int(width) if width else 999
            h = int(height) if height else 999
        except (ValueError, TypeError):
            w, h = 999, 999
        if w < 80 or h < 80:
            continue
        return src

    return ""


def _fetch_with_browser(url: str) -> dict | None:
    try:
        from browser_scraper import fetch_page_with_browser
        return fetch_page_with_browser(url)
    except ImportError:
        logger.warning("[SCRAPER] browser_scraper not available")
        return None
    except Exception as e:
        logger.warning(f"[SCRAPER] Browser fetch failed for {url}: {e}")
        return None


def extract_product_data(url: str, force_browser: bool = False) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    browser_images = []

    try:
        if force_browser:
            browser_result = _fetch_with_browser(url)
            if browser_result:
                html_text = browser_result["html"]
                browser_images = browser_result.get("images", [])
            else:
                with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                html_text = response.text
        else:
            with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
            html_text = response.text

        soup = BeautifulSoup(html_text, "lxml")
        jsonld = extract_jsonld(soup)
        og = extract_og_meta(soup)

        is_valid_product, page_reasons = strict_is_product_page(url, soup)
        if not is_valid_product:
            rejection = page_reasons.get("rejection_reason", "unknown")
            logger.info(f"[PAGE_VALIDATOR] Skipping non-product page: {url} => {rejection}")
            return None
        microdata = extract_microdata(soup)

        name = ""
        brand = ""
        currency = "EUR"
        availability = ""
        image_url = ""

        if jsonld:
            name = jsonld.get("name", "")
            brand_data = jsonld.get("brand", {})
            brand = brand_data.get("name", "") if isinstance(brand_data, dict) else str(brand_data)

            jld_image = jsonld.get("image", "")
            if isinstance(jld_image, list):
                image_url = jld_image[0] if jld_image else ""
            elif isinstance(jld_image, dict):
                image_url = jld_image.get("url", "") or jld_image.get("contentUrl", "")
            else:
                image_url = str(jld_image) if jld_image else ""

            offers = jsonld.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

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
        if not brand:
            brand = extract_brand_from_text(name, url)
        if not image_url:
            image_url = og.get("og:image", "") or microdata.get("image", "")
        if not image_url:
            image_url = _extract_product_image_fallback(soup)
        if image_url and not image_url.startswith("http"):
            from urllib.parse import urljoin
            image_url = urljoin(url, image_url)
        if not currency or currency == "EUR":
            currency = extract_currency(soup)
        if not availability:
            availability = og.get("product:availability", "") or microdata.get("availability", "")
            if availability:
                availability = availability.split("/")[-1]

        price, price_source = extract_price(soup)
        price = validate_price(price)

        needs_js = detect_needs_js_render(soup, has_price=(price is not None))

        bad_titles = ["amazon.fr", "amazon", "decathlon", "cdiscount", "google"]
        if name and name.strip().lower() in bad_titles:
            return None
        if name and len(name.strip()) < 5:
            return None
        if not name or not name.strip():
            name = "Produit inconnu"

        whey_keywords = ["whey", "proteine", "protéine", "protein", "isolat", "isolate", "native", "hydrolys"]
        name_lower = name.lower()
        url_lower = url.lower()
        if not any(kw in name_lower or kw in url_lower for kw in whey_keywords):
            page_title = (soup.find("title") or soup.new_tag("title")).get_text(strip=True).lower() if soup.find("title") else ""
            h1 = soup.find("h1")
            h1_text = h1.get_text(strip=True).lower() if h1 else ""
            if not any(kw in page_title or kw in h1_text for kw in whey_keywords):
                logger.info(f"Skipping non-whey product: {name} ({url})")
                return None

        weight = extract_weight_kg(soup, name, jsonld)
        weight = validate_weight(weight)

        price_per_kg = validate_price_per_kg(price, weight)

        from multi_source_extractor import extract_all_nutrition

        og_data = {}
        for meta in soup.find_all("meta", attrs={"property": True}):
            og_data[meta.get("property", "")] = meta.get("content", "")

        multi_result = extract_all_nutrition(
            soup=soup,
            jsonld=jsonld,
            og=og_data,
            page_url=url,
            enable_ocr=True,
            force_ocr=False,
            extra_images=browser_images if browser_images else None,
        )

        nutrition = multi_result.get("nutrition", {})
        amino_profile = multi_result.get("amino_profile", {})
        amino_base = multi_result.get("amino_base", "unknown")
        sources_used = multi_result.get("sources_used", [])
        raw_evidence = multi_result.get("raw_evidence", [])
        macro_coherent = multi_result.get("macro_coherent", True)

        protein_per_100g = nutrition.get("protein_per_100g")
        protein_source = ",".join(sources_used) if sources_used else None
        protein_confidence = 0.0
        protein_suspect = False

        if protein_per_100g is not None:
            protein_ev = [e for e in raw_evidence if e.get("field") == "protein_per_100g"]
            if protein_ev:
                protein_confidence = max(e.get("confidence", 0) for e in protein_ev)
            if protein_per_100g >= 96 or protein_per_100g < 15:
                protein_suspect = True
        else:
            from nutrition_extractor import extract_protein_per_100g as _extract_protein, extract_protein_from_jsonld

            jsonld_protein = extract_protein_from_jsonld(jsonld)
            if jsonld_protein["protein_per_100g"]:
                protein_per_100g = jsonld_protein["protein_per_100g"]
                protein_source = jsonld_protein["protein_source"]
                protein_confidence = jsonld_protein["protein_confidence"]
                protein_suspect = jsonld_protein["protein_suspect"]
            else:
                html_protein = _extract_protein(soup)
                if html_protein["protein_per_100g"]:
                    protein_per_100g = html_protein["protein_per_100g"]
                    protein_source = html_protein["protein_source"]
                    protein_confidence = html_protein["protein_confidence"]

        carbs_per_100g = nutrition.get("carbs_per_100g")
        sugar_per_100g = nutrition.get("sugar_per_100g")
        fat_per_100g = nutrition.get("fat_per_100g")
        sat_fat_per_100g = nutrition.get("sat_fat_per_100g")
        kcal_per_100g = nutrition.get("kcal_per_100g")
        salt_per_100g = nutrition.get("salt_per_100g")
        fiber_per_100g = nutrition.get("fiber_per_100g")

        leucine_g = amino_profile.get("leucine")
        isoleucine_g = amino_profile.get("isoleucine")
        valine_g = amino_profile.get("valine")
        glutamine_g = amino_profile.get("glutamine")
        arginine_g = amino_profile.get("arginine")
        lysine_g = amino_profile.get("lysine")

        page_full_text = soup.get_text(" ", strip=True)
        ingredients_text = find_ingredients_block(page_full_text, soup=soup)

        multi_ingredients = multi_result.get("ingredients_text")
        if multi_ingredients and (not ingredients_text or len(multi_ingredients) > len(ingredients_text)):
            ingredients_text = multi_ingredients

        analysis_text = ingredients_text or page_full_text

        all_text = (name + " " + (jsonld.get("description", "") if jsonld else "") + " " + page_full_text[:3000]).lower()

        sweeteners = detect_sweeteners(analysis_text)
        whey_type = detect_whey_type(all_text[:2000], product_name=name)
        made_in_france = detect_made_in_france(page_full_text)
        origin = extract_origin_label(page_full_text, made_in_france)
        has_aminogram = detect_aminogram(page_full_text) or len(amino_profile) >= 6
        mentions_bcaa = detect_bcaa(page_full_text) or bool(leucine_g and isoleucine_g and valine_g)

        has_artificial_flavors = detect_artificial_flavors(analysis_text)
        has_thickeners = detect_thickeners(analysis_text)
        has_colorants = detect_colorants(analysis_text)
        ingredient_count = count_ingredients(ingredients_text)

        if not leucine_g or not isoleucine_g or not valine_g:
            legacy_amino = extract_amino_values(page_full_text, protein_per_100g)
            leucine_g = leucine_g or legacy_amino.get("leucine_g")
            isoleucine_g = isoleucine_g or legacy_amino.get("isoleucine_g")
            valine_g = valine_g or legacy_amino.get("valine_g")
            bcaa_per_100g_prot = legacy_amino.get("bcaa_per_100g_prot")

        if leucine_g and isoleucine_g and valine_g:
            bcaa_per_100g_prot = _compute_bcaa_per_100g_prot(
                leucine_g, isoleucine_g, valine_g, protein_per_100g, amino_base
            )
        elif not bcaa_per_100g_prot:
            bcaa_per_100g_prot = None

        price_score = calculate_price_score(price_per_kg)

        effective_protein = None if protein_suspect else protein_per_100g

        protein_score_result = calculate_protein_score(
            protein_per_100g=effective_protein,
            bcaa_per_100g_prot=bcaa_per_100g_prot,
            leucine_g=leucine_g,
            isoleucine_g=isoleucine_g,
            valine_g=valine_g,
        )

        health_result = calculate_health_score(
            has_sucralose=sweeteners.get("sucralose", False),
            has_acesulfame_k=sweeteners.get("acesulfame_k", False),
            has_aspartame=sweeteners.get("aspartame", False),
            has_artificial_flavors=has_artificial_flavors,
            has_thickeners=has_thickeners,
            has_colorants=has_colorants,
            ingredient_count=ingredient_count,
        )

        global_score = calculate_global_score(
            protein_score_result["score_proteique"],
            health_result["score_sante"],
        )

        final_result = calculate_final_score_10(
            score_proteique=protein_score_result["score_proteique"],
            score_sante=health_result["score_sante"],
            price_per_kg=price_per_kg,
            protein_per_100g=effective_protein,
            leucine_g=leucine_g,
            has_aminogram=has_aminogram,
            origin_label=origin["origin_label"],
            bcaa_missing=protein_score_result.get("bcaa_missing", False),
            leucine_missing=protein_score_result.get("leucine_missing", False),
            ingredient_count=ingredient_count,
        )

        result = {
            "nom": name.strip(),
            "marque": brand.strip() if brand else "",
            "url": url,
            "prix": price,
            "devise": currency,
            "disponibilite": availability,
            "poids_kg": weight,
            "prix_par_kg": price_per_kg,
            "proteines_100g": protein_per_100g,
            "protein_source": protein_source,
            "protein_confidence": protein_confidence,
            "protein_suspect": protein_suspect,
            "type_whey": whey_type,
            "made_in_france": made_in_france,
            "origin_label": origin["origin_label"],
            "origin_confidence": origin["origin_confidence"],
            "has_sucralose": sweeteners.get("sucralose", False),
            "has_acesulfame_k": sweeteners.get("acesulfame_k", False),
            "has_aspartame": sweeteners.get("aspartame", False),
            "has_aminogram": has_aminogram,
            "mentions_bcaa": mentions_bcaa,
            "has_artificial_flavors": has_artificial_flavors,
            "has_thickeners": has_thickeners,
            "has_colorants": has_colorants,
            "ingredient_count": ingredient_count,
            "bcaa_per_100g_prot": bcaa_per_100g_prot,
            "leucine_g": leucine_g,
            "isoleucine_g": isoleucine_g,
            "valine_g": valine_g,
            "glutamine_g": glutamine_g,
            "arginine_g": arginine_g,
            "lysine_g": lysine_g,
            "profil_suspect": protein_score_result["profil_suspect"],
            "ingredients": ingredients_text,
            "score_proteique": protein_score_result["score_proteique"],
            "score_sante": health_result["score_sante"],
            "score_prix": price_score,
            "score_global": global_score,
            "score_final": final_result["score_final"],
            "price_score_10": final_result["price_score_10"],
            "is_top_qualite": final_result["is_top_qualite"],
            "is_low_transparency": final_result["is_low_transparency"],
            "image_url": image_url if image_url else None,
            "carbs_per_100g": carbs_per_100g,
            "sugar_per_100g": sugar_per_100g,
            "fat_per_100g": fat_per_100g,
            "sat_fat_per_100g": sat_fat_per_100g,
            "kcal_per_100g": kcal_per_100g,
            "salt_per_100g": salt_per_100g,
            "fiber_per_100g": fiber_per_100g,
            "amino_profile": amino_profile if amino_profile else None,
            "amino_base": amino_base,
            "raw_evidence": raw_evidence if raw_evidence else None,
            "nutrition_sources": ",".join(sources_used) if sources_used else None,
            "macro_coherent": macro_coherent,
            "date_recuperation": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "_has_jsonld": jsonld is not None,
            "_price_source": price_source,
            "_needs_js_render": needs_js,
            "_used_browser": force_browser,
        }

        # --- Retry avec browser si page JS-heavy et données critiques manquantes ---
        if not force_browser and needs_js:
            missing_critical = (
                result["proteines_100g"] is None
                or result["prix"] is None
                or not result["ingredients"]
            )
            if missing_critical:
                logger.info(f"[SCRAPER] JS-heavy page with missing data, retrying with browser: {url}")
                browser_data = _fetch_with_browser(url)
                if browser_data and browser_data.get("html"):
                    browser_soup = BeautifulSoup(browser_data["html"], "lxml")
                    browser_jsonld = extract_jsonld(browser_soup)
                    browser_og = extract_og_meta(browser_soup)
                    browser_price, browser_price_source = extract_price(browser_soup)
                    browser_price = validate_price(browser_price)
                    if browser_price and not result["prix"]:
                        result["prix"] = browser_price
                        result["_price_source"] = browser_price_source
                    if not result["proteines_100g"]:
                        browser_og_data = {}
                        for meta in browser_soup.find_all("meta", attrs={"property": True}):
                            browser_og_data[meta.get("property", "")] = meta.get("content", "")
                        browser_multi = extract_all_nutrition(
                            soup=browser_soup, jsonld=browser_jsonld, og=browser_og_data,
                            page_url=url, enable_ocr=True, force_ocr=False,
                            extra_images=browser_data.get("images"),
                        )
                        browser_protein = browser_multi.get("nutrition", {}).get("protein_per_100g")
                        if browser_protein and browser_protein >= 10 and browser_protein < 96:
                            result["proteines_100g"] = browser_protein
                    if not result["ingredients"]:
                        browser_text = browser_soup.get_text(" ", strip=True)
                        browser_ingredients = find_ingredients_block(browser_text, soup=browser_soup)
                        if browser_ingredients:
                            result["ingredients"] = browser_ingredients
                    browser_img = browser_og.get("og:image", "")
                    if not browser_img and browser_jsonld:
                        jimg = browser_jsonld.get("image", "")
                        browser_img = jimg[0] if isinstance(jimg, list) and jimg else str(jimg) if jimg else ""
                    if browser_img and not result.get("image_url"):
                        result["image_url"] = browser_img
                    result["_used_browser"] = True
                    logger.info(f"[SCRAPER] Browser retry completed for {url}")

        if browser_images:
            result["_gallery_images"] = browser_images[:10]

        return result

    except Exception as e:
        logger.warning(f"Error extracting data from {url}: {e}")
        return None


def compute_confidence(data: dict, has_jsonld: bool) -> float:
    score = 0.0
    factors = 0

    if has_jsonld:
        score += 0.9
    else:
        score += 0.4
    factors += 1

    if data.get("prix") is not None:
        score += 0.8
    else:
        score += 0.1
    factors += 1

    if data.get("poids_kg") is not None:
        score += 0.7
    else:
        score += 0.2
    factors += 1

    if data.get("proteines_100g") is not None:
        score += 0.8
    else:
        score += 0.2
    factors += 1

    if data.get("prix_par_kg") is not None:
        ppk = data["prix_par_kg"]
        if 10 <= ppk <= 100:
            score += 0.9
        elif ppk <= 150:
            score += 0.5
        else:
            score += 0.2
    factors += 1

    nom = data.get("nom", "")
    if nom and len(nom) > 10:
        score += 0.6
    elif nom:
        score += 0.3
    factors += 1

    confidence = round(score / factors, 2) if factors > 0 else 0.3
    return min(1.0, max(0.0, confidence))


def split_product_offer(raw: dict) -> tuple[dict, dict]:
    product_data = {
        "name": raw.get("nom", ""),
        "brand": raw.get("marque", ""),
        "type_whey": raw.get("type_whey", "unknown"),
        "proteines_100g": raw.get("proteines_100g"),
        "bcaa_per_100g_prot": raw.get("bcaa_per_100g_prot"),
        "leucine_g": raw.get("leucine_g"),
        "isoleucine_g": raw.get("isoleucine_g"),
        "valine_g": raw.get("valine_g"),
        "has_aminogram": raw.get("has_aminogram", False),
        "mentions_bcaa": raw.get("mentions_bcaa", False),
        "ingredients": raw.get("ingredients"),
        "ingredient_count": raw.get("ingredient_count"),
        "has_sucralose": raw.get("has_sucralose", False),
        "has_acesulfame_k": raw.get("has_acesulfame_k", False),
        "has_aspartame": raw.get("has_aspartame", False),
        "has_artificial_flavors": raw.get("has_artificial_flavors", False),
        "has_thickeners": raw.get("has_thickeners", False),
        "has_colorants": raw.get("has_colorants", False),
        "origin_label": raw.get("origin_label", "Inconnu"),
        "origin_confidence": raw.get("origin_confidence", 0.3),
        "made_in_france": raw.get("made_in_france", False),
        "profil_suspect": raw.get("profil_suspect", False),
        "protein_source": raw.get("protein_source"),
        "protein_confidence": raw.get("protein_confidence", 0.0),
        "protein_suspect": raw.get("protein_suspect", False),
        "score_proteique": raw.get("score_proteique"),
        "score_sante": raw.get("score_sante"),
        "score_global": raw.get("score_global"),
        "score_final": raw.get("score_final"),
        "image_url": raw.get("image_url"),
        "carbs_per_100g": raw.get("carbs_per_100g"),
        "sugar_per_100g": raw.get("sugar_per_100g"),
        "fat_per_100g": raw.get("fat_per_100g"),
        "sat_fat_per_100g": raw.get("sat_fat_per_100g"),
        "kcal_per_100g": raw.get("kcal_per_100g"),
        "salt_per_100g": raw.get("salt_per_100g"),
        "fiber_per_100g": raw.get("fiber_per_100g"),
        "amino_profile": raw.get("amino_profile"),
        "amino_base": raw.get("amino_base", "unknown"),
        "raw_evidence": raw.get("raw_evidence"),
        "nutrition_sources": raw.get("nutrition_sources"),
        "macro_coherent": raw.get("macro_coherent", True),
        "glutamine_g": raw.get("glutamine_g"),
        "arginine_g": raw.get("arginine_g"),
        "lysine_g": raw.get("lysine_g"),
    }

    merchant = urlparse(raw.get("url", "")).netloc.replace("www.", "")

    offer_data = {
        "merchant": merchant,
        "url": raw.get("url", ""),
        "prix": raw.get("prix"),
        "devise": raw.get("devise", "EUR"),
        "poids_kg": raw.get("poids_kg"),
        "prix_par_kg": raw.get("prix_par_kg"),
        "disponibilite": raw.get("disponibilite", ""),
        "confidence": raw.get("confidence", 0.5),
        "needs_js_render": raw.get("_needs_js_render", False),
        "price_source": raw.get("_price_source", "none"),
    }

    return product_data, offer_data


def _extract_with_log(url: str) -> dict | None:
    try:
        return extract_product_data(url)
    except Exception as e:
        logger.warning(f"Parallel extraction error for {url}: {e}")
        return None


def _extract_with_whey_validation(url: str, use_resolver: bool = True) -> dict | None:
    from page_validator import is_whey_product_page as check_whey
    from resolver import resolve_best_product_url

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }
        with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
            response = client.get(url, headers=headers)
            if response.status_code != 200:
                return None

        is_whey, whey_info = check_whey(response.text, url)
        page_type = whey_info.get("page_type", "unknown")

        if is_whey:
            result = extract_product_data(url)
            if result:
                result["_whey_validated"] = True
                result["_whey_signals"] = whey_info.get("whey_signals", [])
                result["_page_type"] = "product"
            return result

        if use_resolver and page_type in ("article", "unknown", "category"):
            resolved = resolve_best_product_url(
                url,
                start_html=response.text,
                start_page_type=page_type,
            )
            if resolved.get("resolved_url") and resolved["resolved_url"] != url:
                resolved_url = resolved["resolved_url"]
                logger.info(f"[DISCOVERY] Resolved {url} (type={page_type}) => {resolved_url}")
                result = extract_product_data(resolved_url)
                if result:
                    result["url"] = resolved_url
                    result["_whey_validated"] = True
                    result["_resolved_from"] = url
                    result["_resolved_from_type"] = page_type
                    result["_whey_signals"] = []
                    result["_page_type"] = "product"
                return result

        rejection = whey_info.get("rejection_reason", "")
        logger.info(f"[DISCOVERY] Rejected URL (type={page_type}): {url} => {rejection}")
        return None

    except Exception as e:
        logger.warning(f"Whey-validated extraction error for {url}: {e}")
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

    products.sort(key=lambda p: p.get("score_final") or p.get("score_global") or -1, reverse=True)

    return products


def _search_discovery_queries(api_key: str, discovery_queries: list[dict],
                              max_per_domain: int = MAX_PER_DOMAIN,
                              progress_callback=None) -> list[dict]:
    all_urls = []
    seen = set()
    domain_counts: dict[str, int] = {}
    url_sources: dict[str, str] = {}

    for i, qdata in enumerate(discovery_queries):
        query = qdata["query"]
        source = qdata["source"]

        if progress_callback:
            progress_callback(i, len(discovery_queries), f"Requete: {source}")

        urls = search_brave(api_key, query)
        for u in urls:
            normalized = u.rstrip("/").lower()
            if normalized in seen:
                continue

            bad, bad_reason = is_bad_url(u)
            if bad:
                logger.debug(f"[DISCOVERY] Pre-filter rejected URL: {u} => {bad_reason}")
                continue

            domain = urlparse(u).netloc.replace("www.", "").lower()

            if domain_counts.get(domain, 0) >= max_per_domain:
                continue

            seen.add(normalized)
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
            all_urls.append({"url": u, "source": source, "domain": domain})
            url_sources[normalized] = source

        time.sleep(REQUEST_DELAY)

    return all_urls


def run_discovery(api_key: str, progress_callback=None, status_callback=None,
                  max_per_domain: int = MAX_PER_DOMAIN,
                  use_brand_seeds: bool = True,
                  block_domains: list[str] | None = None,
                  scrape_limit: int = 200,
                  use_whey_filter: bool = True,
                  use_resolver: bool = True) -> dict:
    from db import upsert_product, upsert_offer, create_pipeline_run, update_pipeline_run
    from db import add_product_image

    run_id = create_pipeline_run("discovery")
    effective_block = block_domains if block_domains is not None else BLOCK_DOMAINS
    stats = {
        "products_found": 0,
        "offers_created": 0,
        "errors": 0,
        "skipped": 0,
        "resolved": 0,
        "whey_rejected": 0,
        "domains_found": set(),
        "brands_found": set(),
        "brands_missing": set(),
        "domain_counts": {},
    }

    try:
        if status_callback:
            status_callback("Generation des requetes discovery...")

        discovery_queries = generate_discovery_queries(
            use_brand_seeds=use_brand_seeds,
            block_domains=effective_block,
        )

        if status_callback:
            status_callback(f"{len(discovery_queries)} requetes a executer via Brave Search...")

        url_entries = _search_discovery_queries(
            api_key, discovery_queries,
            max_per_domain=max_per_domain,
            progress_callback=progress_callback,
        )

        if scrape_limit and len(url_entries) > scrape_limit:
            url_entries = url_entries[:scrape_limit]

        if status_callback:
            unique_domains = len(set(e["domain"] for e in url_entries))
            status_callback(f"{len(url_entries)} URLs candidates ({unique_domains} domaines). Extraction...")

        completed = 0
        total = len(url_entries)
        url_source_map = {e["url"]: e["source"] for e in url_entries}
        urls_to_scrape = [e["url"] for e in url_entries]

        if use_whey_filter:
            extract_fn = lambda url: _extract_with_whey_validation(url, use_resolver=use_resolver)
        else:
            extract_fn = _extract_with_log

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extract_fn, url): url for url in urls_to_scrape}

            for future in as_completed(futures):
                completed += 1
                url = futures[future]
                source = url_source_map.get(url, "unknown")
                domain = urlparse(url).netloc.replace("www.", "").lower()

                if progress_callback:
                    progress_callback(completed, total, f"Extraction: {domain}")

                result = future.result()
                if result is None:
                    if use_whey_filter:
                        stats["whey_rejected"] += 1
                    else:
                        stats["skipped"] += 1
                    continue

                if result.get("_resolved_from"):
                    stats["resolved"] += 1

                try:
                    needs_js = result.get("_needs_js_render", False)
                    confidence = compute_confidence_v2(
                        result,
                        has_jsonld=bool(result.get("_has_jsonld", False)),
                        needs_js_render=needs_js,
                    )
                    result["confidence"] = confidence

                    if confidence < 0.2:
                        stats["skipped"] += 1
                        continue

                    product_data, offer_data = split_product_offer(result)
                    product_data["needs_review"] = confidence < 0.5 or needs_js
                    offer_data["discovery_source"] = source

                    product_id = upsert_product(product_data)
                    upsert_offer(product_id, offer_data)

                    gallery = result.get("_gallery_images", [])
                    for gi, img_url in enumerate(gallery):
                        try:
                            add_product_image(product_id, img_url, sort_order=gi)
                        except Exception:
                            pass

                    stats["products_found"] += 1
                    stats["offers_created"] += 1
                    stats["domains_found"].add(domain)
                    stats["domain_counts"][domain] = stats["domain_counts"].get(domain, 0) + 1

                    brand = result.get("marque", "").strip()
                    if brand:
                        stats["brands_found"].add(brand.lower())

                except Exception as e:
                    logger.warning(f"Error saving product/offer for {url}: {e}")
                    stats["errors"] += 1

        seed_brands_lower = {b.lower() for b in SEED_BRANDS.keys()}
        stats["brands_missing"] = seed_brands_lower - stats["brands_found"]

        stats["domains_found"] = list(stats["domains_found"])
        stats["brands_found"] = list(stats["brands_found"])
        stats["brands_missing"] = list(stats["brands_missing"])

        details = (
            f"URLs scannees: {total}, Ignores: {stats['skipped']}, "
            f"Whey rejetes: {stats['whey_rejected']}, Resolus: {stats['resolved']}, "
            f"Domaines uniques: {len(stats['domains_found'])}, "
            f"Marques trouvees: {len(stats['brands_found'])}, "
            f"Marques manquantes: {len(stats['brands_missing'])} ({', '.join(stats['brands_missing'][:10])})"
        )

        update_pipeline_run(
            run_id, "completed",
            products_found=stats["products_found"],
            offers_updated=stats["offers_created"],
            errors=stats["errors"],
            details=details,
        )

    except Exception as e:
        logger.error(f"Discovery pipeline error: {e}")
        update_pipeline_run(run_id, "failed", errors=1, details=str(e))
        raise

    return stats


def get_discovery_stats_from_db() -> dict:
    from db import get_connection
    import psycopg2.extras

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT o.merchant, COUNT(*) AS offer_count, 
                   COUNT(DISTINCT o.product_id) AS product_count,
                   AVG(o.confidence) AS avg_confidence
            FROM offers o
            WHERE o.is_active = TRUE
            GROUP BY o.merchant
            ORDER BY offer_count DESC
        """)
        domain_stats = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT LOWER(p.brand) AS brand, COUNT(*) AS product_count
            FROM products p
            WHERE p.brand IS NOT NULL AND p.brand != ''
            GROUP BY LOWER(p.brand)
            ORDER BY product_count DESC
        """)
        brand_rows = [dict(r) for r in cur.fetchall()]
        brands_in_db = {r["brand"] for r in brand_rows}

        seed_brands_lower = {b.lower() for b in SEED_BRANDS.keys()}
        brands_missing = seed_brands_lower - brands_in_db
        brands_found = seed_brands_lower & brands_in_db

        cur.execute("SELECT COUNT(DISTINCT merchant) AS cnt FROM offers WHERE is_active = TRUE")
        unique_domains = cur.fetchone()["cnt"]

        return {
            "unique_domains": unique_domains,
            "domain_stats": domain_stats[:20],
            "brands_in_catalog": sorted(brands_found),
            "brands_missing": sorted(brands_missing),
            "brand_details": brand_rows[:30],
            "total_seed_brands": len(SEED_BRANDS),
        }
    finally:
        cur.close()
        conn.close()


def refresh_offer_price(url: str) -> dict | None:
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

        price, price_source = extract_price(soup)
        price = validate_price(price)
        weight = extract_weight_kg(soup, "", jsonld)
        weight = validate_weight(weight)

        price_per_kg = validate_price_per_kg(price, weight)

        needs_js = detect_needs_js_render(soup, has_price=(price is not None))

        availability = ""
        if jsonld:
            offers = jsonld.get("offers", {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            availability = offers.get("availability", "")
            if availability:
                availability = availability.split("/")[-1]

        has_jsonld = jsonld is not None
        refresh_data = {
            "nom": "refresh",
            "prix": price,
            "poids_kg": weight,
            "prix_par_kg": price_per_kg,
            "proteines_100g": None,
        }
        confidence = compute_confidence_v2(refresh_data, has_jsonld=has_jsonld, needs_js_render=needs_js)

        image_url = ""
        if jsonld:
            jld_image = jsonld.get("image", "")
            if isinstance(jld_image, list):
                image_url = jld_image[0] if jld_image else ""
            elif isinstance(jld_image, dict):
                image_url = jld_image.get("url", "") or jld_image.get("contentUrl", "")
            else:
                image_url = str(jld_image) if jld_image else ""
        if not image_url:
            og = extract_og_meta(soup)
            image_url = og.get("og:image", "")
        if not image_url:
            image_url = _extract_product_image_fallback(soup)
        if image_url and not image_url.startswith("http"):
            from urllib.parse import urljoin
            image_url = urljoin(url, image_url)

        return {
            "prix": price,
            "prix_par_kg": price_per_kg,
            "disponibilite": availability,
            "confidence": confidence,
            "needs_js_render": needs_js,
            "price_source": price_source,
            "image_url": image_url if image_url else None,
        }

    except Exception as e:
        logger.warning(f"Refresh error for {url}: {e}")
        return None


def reanalyze_product_nutrition(product_id: int, offer_url: str) -> dict | None:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }

        with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True, verify=False) as client:
            response = client.get(offer_url, headers=headers)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        jsonld = extract_jsonld(soup)

        from multi_source_extractor import extract_all_nutrition

        og_data = {}
        for meta in soup.find_all("meta", attrs={"property": True}):
            og_data[meta.get("property", "")] = meta.get("content", "")

        multi_result = extract_all_nutrition(
            soup=soup, jsonld=jsonld, og=og_data,
            page_url=offer_url, enable_ocr=True, force_ocr=True,
        )

        nutrition = multi_result.get("nutrition", {})
        amino_profile = multi_result.get("amino_profile", {})
        sources_used = multi_result.get("sources_used", [])

        page_text = soup.get_text(" ", strip=True)
        ingredients_text = find_ingredients_block(page_text, soup=soup)
        multi_ingredients = multi_result.get("ingredients_text")
        if multi_ingredients and (not ingredients_text or len(multi_ingredients) > len(ingredients_text)):
            ingredients_text = multi_ingredients

        protein_per_100g = nutrition.get("protein_per_100g")

        leucine_g = amino_profile.get("leucine")
        isoleucine_g = amino_profile.get("isoleucine")
        valine_g = amino_profile.get("valine")

        if not leucine_g or not isoleucine_g or not valine_g:
            legacy = extract_amino_values(page_text, protein_per_100g)
            leucine_g = leucine_g or legacy.get("leucine_g")
            isoleucine_g = isoleucine_g or legacy.get("isoleucine_g")
            valine_g = valine_g or legacy.get("valine_g")

        update_data = {}

        if protein_per_100g is not None:
            update_data["proteines_100g"] = protein_per_100g
        if nutrition.get("carbs_per_100g") is not None:
            update_data["carbs_per_100g"] = nutrition["carbs_per_100g"]
        if nutrition.get("sugar_per_100g") is not None:
            update_data["sugar_per_100g"] = nutrition["sugar_per_100g"]
        if nutrition.get("fat_per_100g") is not None:
            update_data["fat_per_100g"] = nutrition["fat_per_100g"]
        if nutrition.get("sat_fat_per_100g") is not None:
            update_data["sat_fat_per_100g"] = nutrition["sat_fat_per_100g"]
        if nutrition.get("kcal_per_100g") is not None:
            update_data["kcal_per_100g"] = nutrition["kcal_per_100g"]
        if nutrition.get("salt_per_100g") is not None:
            update_data["salt_per_100g"] = nutrition["salt_per_100g"]
        if nutrition.get("fiber_per_100g") is not None:
            update_data["fiber_per_100g"] = nutrition["fiber_per_100g"]

        if amino_profile:
            update_data["amino_profile"] = amino_profile
        update_data["amino_base"] = multi_result.get("amino_base", "unknown")

        if leucine_g:
            update_data["leucine_g"] = leucine_g
        if isoleucine_g:
            update_data["isoleucine_g"] = isoleucine_g
        if valine_g:
            update_data["valine_g"] = valine_g
        if amino_profile.get("glutamine"):
            update_data["glutamine_g"] = amino_profile["glutamine"]
        if amino_profile.get("arginine"):
            update_data["arginine_g"] = amino_profile["arginine"]
        if amino_profile.get("lysine"):
            update_data["lysine_g"] = amino_profile["lysine"]

        has_aminogram = len(amino_profile) >= 6 or bool(leucine_g and isoleucine_g and valine_g)
        update_data["has_aminogram"] = has_aminogram

        if leucine_g and isoleucine_g and valine_g:
            amino_base = multi_result.get("amino_base", "unknown")
            bcaa_val = _compute_bcaa_per_100g_prot(
                leucine_g, isoleucine_g, valine_g, protein_per_100g, amino_base
            )
            if bcaa_val is not None:
                update_data["bcaa_per_100g_prot"] = bcaa_val
            update_data["mentions_bcaa"] = True

        raw_evidence = multi_result.get("raw_evidence", [])
        if raw_evidence:
            update_data["raw_evidence"] = raw_evidence
        update_data["nutrition_sources"] = ",".join(sources_used) if sources_used else None
        update_data["macro_coherent"] = multi_result.get("macro_coherent", True)

        if ingredients_text:
            analysis_text = ingredients_text
            sweeteners = detect_sweeteners(analysis_text)
            update_data["has_sucralose"] = sweeteners.get("sucralose", False)
            update_data["has_acesulfame_k"] = sweeteners.get("acesulfame_k", False)
            update_data["has_aspartame"] = sweeteners.get("aspartame", False)
            update_data["has_artificial_flavors"] = detect_artificial_flavors(analysis_text)
            update_data["has_thickeners"] = detect_thickeners(analysis_text)
            update_data["has_colorants"] = detect_colorants(analysis_text)
            update_data["ingredient_count"] = count_ingredients(ingredients_text)
            update_data["ingredients"] = ingredients_text

        bcaa_val = update_data.get("bcaa_per_100g_prot")
        eff_protein = protein_per_100g
        protein_score_result = calculate_protein_score(
            protein_per_100g=eff_protein,
            bcaa_per_100g_prot=bcaa_val,
            leucine_g=leucine_g,
            isoleucine_g=isoleucine_g,
            valine_g=valine_g,
        )
        update_data["score_proteique"] = protein_score_result["score_proteique"]

        health_result = calculate_health_score(
            has_sucralose=update_data.get("has_sucralose", False),
            has_acesulfame_k=update_data.get("has_acesulfame_k", False),
            has_aspartame=update_data.get("has_aspartame", False),
            has_artificial_flavors=update_data.get("has_artificial_flavors", False),
            has_thickeners=update_data.get("has_thickeners", False),
            has_colorants=update_data.get("has_colorants", False),
            ingredient_count=update_data.get("ingredient_count", 0),
        )
        update_data["score_sante"] = health_result["score_sante"]

        from db import get_connection
        conn_score = get_connection()
        cur_score = conn_score.cursor()
        try:
            cur_score.execute("SELECT prix_par_kg FROM offers WHERE product_id = %s AND is_active = TRUE ORDER BY prix_par_kg ASC LIMIT 1", (product_id,))
            row = cur_score.fetchone()
            price_per_kg = row[0] if row else None
        finally:
            cur_score.close()
            conn_score.close()

        global_score = calculate_global_score(
            protein_score_result["score_proteique"],
            health_result["score_sante"],
        )
        update_data["score_global"] = global_score

        final_result = calculate_final_score_10(
            score_proteique=protein_score_result["score_proteique"],
            score_sante=health_result["score_sante"],
            price_per_kg=price_per_kg,
            protein_per_100g=protein_per_100g,
            leucine_g=leucine_g,
            has_aminogram=has_aminogram,
            bcaa_missing=protein_score_result.get("bcaa_missing", False),
            leucine_missing=protein_score_result.get("leucine_missing", False),
            ingredient_count=update_data.get("ingredient_count"),
        )
        update_data["score_final"] = final_result["score_final"]

        return update_data

    except Exception as e:
        logger.warning(f"Re-analysis error for product {product_id} ({offer_url}): {e}")
        return None


def run_reanalysis(progress_callback=None, status_callback=None) -> dict:
    from db import get_connection, get_product_offers
    import psycopg2.extras
    import json as _json

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute("""
            SELECT p.id, p.name FROM products p
            WHERE p.amino_profile IS NULL OR p.kcal_per_100g IS NULL
            ORDER BY p.id
        """)
        products_to_update = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    stats = {"updated": 0, "failed": 0, "skipped": 0, "total": len(products_to_update)}

    if status_callback:
        status_callback(f"Re-analyse de {len(products_to_update)} produits...")

    for i, prod in enumerate(products_to_update):
        if progress_callback:
            progress_callback(i + 1, len(products_to_update), f"Re-analyse: {prod['name'][:40]}")

        offers = get_product_offers(prod["id"])
        active_offers = [o for o in offers if o.get("is_active", True)]

        if not active_offers:
            stats["skipped"] += 1
            continue

        best_offer = active_offers[0]
        result = reanalyze_product_nutrition(prod["id"], best_offer["url"])

        if result and len(result) > 2:
            conn2 = get_connection()
            cur2 = conn2.cursor()
            try:
                set_parts = []
                vals = []
                for k, v in result.items():
                    if k in ("amino_profile", "raw_evidence") and v is not None:
                        set_parts.append(f"{k} = %s")
                        vals.append(_json.dumps(v))
                    else:
                        set_parts.append(f"{k} = %s")
                        vals.append(v)

                set_parts.append("updated_at = NOW()")
                vals.append(prod["id"])

                sql = f"UPDATE products SET {', '.join(set_parts)} WHERE id = %s"
                cur2.execute(sql, vals)
                conn2.commit()
                stats["updated"] += 1
            except Exception as e:
                conn2.rollback()
                logger.warning(f"DB update failed for product {prod['id']}: {e}")
                stats["failed"] += 1
            finally:
                cur2.close()
                conn2.close()
        else:
            stats["failed"] += 1

        time.sleep(1)

    return stats


def run_refresh(progress_callback=None, status_callback=None) -> dict:
    from db import get_active_offers, update_offer_price, mark_offer_failed, update_product_image
    from db import create_pipeline_run, update_pipeline_run
    from db import record_price_snapshot, check_and_trigger_alerts

    run_id = create_pipeline_run("refresh")
    stats = {"updated": 0, "failed": 0, "total": 0}

    try:
        offers = get_active_offers(min_confidence=0.3)
        stats["total"] = len(offers)

        if status_callback:
            status_callback(f"Mise a jour de {len(offers)} offres actives...")

        for i, offer in enumerate(offers):
            if progress_callback:
                progress_callback(i + 1, len(offers), f"Refresh: {urlparse(offer['url']).netloc}")

            result = refresh_offer_price(offer["url"])

            if result and result.get("prix") is not None:
                update_offer_price(
                    offer["id"],
                    prix=result["prix"],
                    prix_par_kg=result.get("prix_par_kg"),
                    disponibilite=result.get("disponibilite", ""),
                    confidence=result.get("confidence", 0.5),
                )
                if result.get("image_url") and offer.get("product_id"):
                    try:
                        update_product_image(offer["product_id"], result["image_url"])
                    except Exception:
                        pass
                if offer.get("product_id") and result.get("prix"):
                    try:
                        merchant = offer.get("merchant", "")
                        ppk = result.get("prix_par_kg")
                        record_price_snapshot(offer["product_id"], result["prix"], ppk, merchant)
                        if ppk:
                            check_and_trigger_alerts(offer["product_id"], ppk)
                    except Exception:
                        pass
                stats["updated"] += 1
            else:
                mark_offer_failed(offer["id"])
                stats["failed"] += 1

            time.sleep(0.5)

        update_pipeline_run(
            run_id, "completed",
            offers_updated=stats["updated"],
            errors=stats["failed"],
            details=f"Total offres: {stats['total']}",
        )

    except Exception as e:
        logger.error(f"Refresh pipeline error: {e}")
        update_pipeline_run(run_id, "failed", errors=1, details=str(e))
        raise

    return stats
