import json
import re
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

WHEY_PRICE_MIN = 8.0
WHEY_PRICE_MAX = 200.0

CROSSED_PRICE_TAGS = {"del", "s", "strike"}
CROSSED_PRICE_CLASSES = re.compile(
    r"old|was|regular|crossed|barre|compare|list-price|price--old|price-old|prix-barre|"
    r"price-was|original.?price|retail.?price|rrp|msrp|before",
    re.I,
)
NOISE_CONTEXTS = re.compile(
    r"(?:à\s+partir\s+de|abonnement|livraison|frais\s+de\s+port|"
    r"economisez|remise|reduction|coupon|souscription|subscription|"
    r"shipping|delivery|par\s+mois|/mois)",
    re.I,
)


def _parse_price_value(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if WHEY_PRICE_MIN <= v <= WHEY_PRICE_MAX else None
    text = str(value).replace(",", ".").replace("\u00a0", "").replace(" ", "").strip()
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        v = float(match.group(1))
        return v if WHEY_PRICE_MIN <= v <= WHEY_PRICE_MAX else None
    return None


def _walk_json(obj, key_target: str, max_depth: int = 10) -> list:
    results = []
    if max_depth <= 0:
        return results
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == key_target:
                results.append(v)
            else:
                results.extend(_walk_json(v, key_target, max_depth - 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_walk_json(item, key_target, max_depth - 1))
    return results


def extract_price_jsonld(soup: BeautifulSoup) -> list[tuple[str, float]]:
    prices = []
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        products = []

        def _is_product_type(item):
            if not isinstance(item, dict):
                return False
            t = item.get("@type")
            if isinstance(t, str):
                return t == "Product"
            if isinstance(t, list):
                return "Product" in t
            return False

        if isinstance(data, list):
            products = [d for d in data if _is_product_type(d)]
        elif isinstance(data, dict):
            if _is_product_type(data):
                products.append(data)
            if "@graph" in data:
                products.extend(
                    d for d in data["@graph"]
                    if _is_product_type(d)
                )

        for product in products:
            offers = product.get("offers", {})
            offer_list = offers if isinstance(offers, list) else [offers] if isinstance(offers, dict) else []

            for offer in offer_list:
                if not isinstance(offer, dict):
                    continue
                p = _parse_price_value(offer.get("price"))
                if p:
                    prices.append(("jsonld_offer", p))

                spec = offer.get("priceSpecification")
                if isinstance(spec, dict):
                    p2 = _parse_price_value(spec.get("price"))
                    if p2:
                        prices.append(("jsonld_spec", p2))
                elif isinstance(spec, list):
                    for s in spec:
                        if isinstance(s, dict):
                            p2 = _parse_price_value(s.get("price"))
                            if p2:
                                prices.append(("jsonld_spec", p2))

                low = _parse_price_value(offer.get("lowPrice"))
                if low:
                    prices.append(("jsonld_low", low))

    return prices


def extract_price_meta(soup: BeautifulSoup) -> list[tuple[str, float]]:
    prices = []
    meta_map = {}
    for tag in soup.find_all("meta"):
        prop = (tag.get("property", "") or tag.get("name", "")).lower()
        content = tag.get("content", "")
        if prop and content:
            meta_map[prop] = content

    for key in ("product:price:amount", "og:price:amount"):
        p = _parse_price_value(meta_map.get(key))
        if p:
            prices.append(("meta_" + key.replace(":", "_"), p))

    twitter_data1 = meta_map.get("twitter:data1", "")
    if twitter_data1 and ("€" in twitter_data1 or "eur" in twitter_data1.lower()):
        p = _parse_price_value(twitter_data1)
        if p:
            prices.append(("meta_twitter", p))

    return prices


def extract_price_next_nuxt(soup: BeautifulSoup) -> list[tuple[str, float]]:
    prices = []

    for script in soup.find_all("script", id="__NEXT_DATA__"):
        try:
            data = json.loads(script.string or "")
            found = _walk_json(data, "price", max_depth=15)
            for val in found:
                p = _parse_price_value(val)
                if p:
                    prices.append(("next_data", p))
                    break
        except (json.JSONDecodeError, TypeError):
            continue

    for script in soup.find_all("script"):
        text = script.string or ""
        for pattern_name, regex in [
            ("nuxt", r"window\.__NUXT__\s*=\s*(.+?);\s*(?:</|$)"),
            ("initial_state", r"window\.__INITIAL_STATE__\s*=\s*(.+?);\s*(?:</|$)"),
        ]:
            m = re.search(regex, text, re.S)
            if m:
                try:
                    data = json.loads(m.group(1))
                    found = _walk_json(data, "price", max_depth=15)
                    for val in found:
                        p = _parse_price_value(val)
                        if p:
                            prices.append((pattern_name, p))
                            break
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

        if "product" in text.lower() and "price" in text.lower():
            json_blobs = re.findall(r'\{[^{}]{20,5000}\}', text)
            for blob in json_blobs[:5]:
                try:
                    data = json.loads(blob)
                    if "price" in str(data).lower():
                        found = _walk_json(data, "price", max_depth=5)
                        for val in found:
                            p = _parse_price_value(val)
                            if p:
                                prices.append(("script_json", p))
                                break
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

    return prices


def _is_crossed_price_element(el) -> bool:
    if el.name in CROSSED_PRICE_TAGS:
        return True
    for parent in el.parents:
        if parent.name in CROSSED_PRICE_TAGS:
            return True
        classes = " ".join(parent.get("class", []))
        if CROSSED_PRICE_CLASSES.search(classes):
            return True
        if parent == el.parent:
            break
    classes = " ".join(el.get("class", []))
    if CROSSED_PRICE_CLASSES.search(classes):
        return True
    return False


def extract_price_regex(soup: BeautifulSoup) -> list[tuple[str, float]]:
    prices = []

    priority_classes = [
        "current-price", "sale-price", "product-price", "price--current",
        "price-new", "our-price", "final-price", "price-actual",
    ]
    for cls in priority_classes:
        els = soup.find_all(class_=re.compile(re.escape(cls), re.I))
        for el in els:
            if _is_crossed_price_element(el):
                continue
            p = _parse_price_value(el.get_text())
            if p:
                prices.append(("html_priority", p))
                break
        if prices:
            break

    proximity_patterns = [
        r'(?:prix|price|tarif)\s*(?::\s*)?(\d+[.,]\d{2})\s*€',
        r'(\d+[.,]\d{2})\s*€\s*(?:ttc|ht)',
        r'(\d+[.,]\d{2})\s*€',
        r'€\s*(\d+[.,]\d{2})',
    ]

    body = soup.find("body")
    if body:
        html_text = str(body)

        cart_zones = re.finditer(
            r'(?:ajouter\s+au\s+panier|add\s+to\s+cart|acheter|commander)',
            html_text, re.I,
        )
        for cart_match in cart_zones:
            start = max(0, cart_match.start() - 1500)
            end = min(len(html_text), cart_match.end() + 500)
            zone = html_text[start:end]

            for pat in proximity_patterns:
                m = re.search(pat, zone, re.I)
                if m:
                    val_str = m.group(1).replace(",", ".")
                    p = _parse_price_value(val_str)
                    if p:
                        zone_lower = zone[max(0, m.start() - 100):m.end() + 50].lower()
                        if not NOISE_CONTEXTS.search(zone_lower):
                            crossed_check = zone[max(0, m.start() - 60):m.end() + 10]
                            if not re.search(r'<(?:del|s|strike)\b', crossed_check, re.I):
                                if not re.search(r'class="[^"]*(?:old|was|crossed|barre)', crossed_check, re.I):
                                    prices.append(("regex_cart", p))
                                    break

    if not prices:
        for cls in ["price", "prix"]:
            els = soup.find_all(class_=re.compile(cls, re.I))
            for el in els:
                if _is_crossed_price_element(el):
                    continue
                p = _parse_price_value(el.get_text())
                if p:
                    prices.append(("html_generic", p))
                    break
            if prices:
                break

    return prices


def extract_price(soup: BeautifulSoup) -> tuple[float | None, str]:
    all_prices = []

    jsonld_prices = extract_price_jsonld(soup)
    all_prices.extend(jsonld_prices)

    meta_prices = extract_price_meta(soup)
    all_prices.extend(meta_prices)

    next_nuxt_prices = extract_price_next_nuxt(soup)
    all_prices.extend(next_nuxt_prices)

    regex_prices = extract_price_regex(soup)
    all_prices.extend(regex_prices)

    if not all_prices:
        return None, "none"

    source_priority = {
        "jsonld_offer": 0, "jsonld_spec": 1, "jsonld_low": 2,
        "meta_product_price_amount": 3, "meta_og_price_amount": 4, "meta_twitter": 5,
        "next_data": 6, "nuxt": 7, "initial_state": 8, "script_json": 9,
        "html_priority": 10, "regex_cart": 11, "html_generic": 12,
    }
    all_prices.sort(key=lambda x: source_priority.get(x[0], 99))

    best_source, best_price = all_prices[0]
    return round(best_price, 2), best_source


def extract_currency(soup: BeautifulSoup) -> str:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and item.get("@type") == "Product":
                    offers = item.get("offers", {})
                    offer_list = offers if isinstance(offers, list) else [offers]
                    for o in offer_list:
                        if isinstance(o, dict):
                            cur = o.get("priceCurrency", "")
                            if cur:
                                return cur.upper()
        except (json.JSONDecodeError, TypeError):
            continue

    for tag in soup.find_all("meta"):
        prop = (tag.get("property", "") or tag.get("name", "")).lower()
        content = tag.get("content", "")
        if prop == "product:price:currency" and content:
            return content.upper()

    page_text = soup.get_text(" ", strip=True)
    if "€" in page_text or "EUR" in page_text:
        return "EUR"
    if "£" in page_text or "GBP" in page_text:
        return "GBP"
    if "$" in page_text or "USD" in page_text:
        return "USD"

    return "EUR"


def extract_weight_kg(soup: BeautifulSoup, product_name: str = "", jsonld: dict | None = None) -> float | None:
    candidates = []

    title_text = product_name.lower().replace(",", ".")

    if jsonld:
        desc = jsonld.get("description", "")
        if isinstance(desc, str):
            title_text += " " + desc.lower().replace(",", ".")

    title_w = _parse_weight(title_text)
    if title_w and 0.2 <= title_w <= 5.0:
        candidates.append(("title", title_w))

    if jsonld:
        weight_data = jsonld.get("weight", "")
        if isinstance(weight_data, dict):
            weight_data = weight_data.get("value", "")
        w = _parse_weight(str(weight_data))
        if w and 0.2 <= w <= 5.0:
            candidates.append(("jsonld_weight", w))

    for tag_name in ["title", "h1"]:
        tag = soup.find(tag_name)
        if tag:
            w = _parse_weight(tag.get_text(strip=True))
            if w and 0.2 <= w <= 5.0:
                candidates.append((f"html_{tag_name}", w))

    weight_els = soup.find_all(["span", "div", "p", "li", "select", "option"],
                               class_=re.compile(r"weight|poids|size|taille|format|variant", re.I))
    for el in weight_els:
        w = _parse_weight(el.get_text(strip=True))
        if w and 0.2 <= w <= 5.0:
            candidates.append(("html_class", w))

    if candidates:
        for src, w in candidates:
            if src == "title":
                return w
        return candidates[0][1]

    page_text = soup.get_text(" ", strip=True).lower().replace(",", ".")
    weight_patterns = [
        r"(?:poids|contenance|format|taille|net\s*wt)\s*[:\s]*(\d+(?:\.\d+)?)\s*kg",
        r"(?:poids|contenance|format|taille|net\s*wt)\s*[:\s]*(\d{3,5})\s*g(?:r|ramme)?",
        r"(\d+(?:\.\d+)?)\s*kg\s*(?:de\s+)?(?:whey|prot[ée]ine|poudre)",
    ]
    for pat in weight_patterns:
        m = re.search(pat, page_text)
        if m:
            val = float(m.group(1))
            if "kg" in pat or val < 100:
                if 0.2 <= val <= 5.0:
                    return val
            elif 200 <= val <= 5000:
                return val / 1000

    general_kg = re.findall(r"(\d+(?:\.\d+)?)\s*kg", page_text)
    for v_str in general_kg:
        v = float(v_str)
        if 0.2 <= v <= 5.0:
            return v

    general_g = re.findall(r"(\d{3,5})\s*g(?:r|ramme)?", page_text)
    for v_str in general_g:
        v = float(v_str)
        if 200 <= v <= 5000:
            return v / 1000

    return None


def _parse_weight(text: str) -> float | None:
    if not text:
        return None
    text = text.lower().replace(",", ".")

    m = re.search(r"(\d+(?:\.\d+)?)\s*kg", text)
    if m:
        val = float(m.group(1))
        if 0.1 <= val <= 25:
            return val

    m = re.search(r"(\d+(?:\.\d+)?)\s*g(?:r|ramme)?(?:\b|[^a-z])", text)
    if m:
        grams = float(m.group(1))
        if 100 <= grams <= 25000:
            return grams / 1000

    return None


def detect_needs_js_render(soup: BeautifulSoup, has_price: bool) -> bool:
    if has_price:
        return False

    signals = 0

    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        signals += 2

    for script in soup.find_all("script"):
        src = script.get("src", "")
        text = script.string or ""
        if "next" in src.lower() or "_next/" in src:
            signals += 1
        if "nuxt" in src.lower() or "_nuxt/" in src:
            signals += 1
        if "__NUXT__" in text:
            signals += 2
        if "react" in src.lower() or "React" in text[:500]:
            signals += 1
        if "vue" in src.lower() and "vue" != src.lower():
            signals += 1

    cart_patterns = [
        re.compile(r"ajout.*panier", re.I),
        re.compile(r"add.*cart", re.I),
        re.compile(r"ajouter.*panier", re.I),
    ]
    for pat in cart_patterns:
        btn = soup.find(["button", "a", "input"], string=pat)
        if btn:
            signals += 1
            break
        btn = soup.find(["button", "a"], class_=re.compile(r"add.?to.?cart|ajout.?panier|buy", re.I))
        if btn:
            signals += 1
            break

    body = soup.find("body")
    if body:
        body_text = body.get_text(strip=True)
        if len(body_text) < 200:
            signals += 2

    return signals >= 2
