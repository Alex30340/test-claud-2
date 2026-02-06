import json
import re
import time
import logging
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from scoring import calculate_price_score, calculate_nutrition_score, calculate_global_score

logger = logging.getLogger(__name__)

SEARCH_QUERIES = [
    "whey protein achat",
    "whey isolate prix",
    "whey hydrolysate achat",
    "protéine whey 1kg prix",
    "clear whey isolate achat",
    "protéine musculation whey prix",
]

EXCLUDED_DOMAINS = [
    "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "reddit.com", "forum", "blog", "wikipedia.org",
    "pinterest.com", "linkedin.com", "quora.com",
]

EXCLUDED_PATH_KEYWORDS = [
    "blog", "forum", "article", "comparatif", "avis", "guide",
    "video", "news", "actualite",
]

REQUEST_DELAY = 1.5
HTTP_TIMEOUT = 15.0


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


def search_brave(api_key: str, query: str, count: int = 10) -> list[str]:
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
            response.raise_for_status()
            data = response.json()

        results = []
        for item in data.get("web", {}).get("results", []):
            page_url = item.get("url", "")
            if page_url and is_product_url(page_url):
                results.append(page_url)
        return results

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


def extract_product_data(url: str) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        jsonld = extract_jsonld(soup)

        if not jsonld:
            return None

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

        weight = parse_weight(name) or parse_weight(jsonld.get("description", ""))
        weight_str = jsonld.get("weight", "")
        if not weight and weight_str:
            if isinstance(weight_str, dict):
                weight_str = weight_str.get("value", "")
            weight = parse_weight(str(weight_str))

        price_per_kg = None
        if price and weight and weight > 0:
            price_per_kg = round(price / weight, 2)

        protein_per_100g = None
        nutrition = jsonld.get("nutrition", {})
        if isinstance(nutrition, dict):
            protein_per_100g = parse_protein(nutrition.get("proteinContent", ""))

        if not protein_per_100g:
            page_text = soup.get_text().lower()
            protein_match = re.search(r"prot[ée]ines?\s*[:/]?\s*(\d+(?:[.,]\d+)?)\s*g", page_text)
            if protein_match:
                val = float(protein_match.group(1).replace(",", "."))
                if 0 < val <= 100:
                    protein_per_100g = val

        price_score = calculate_price_score(price_per_kg)
        nutrition_score = calculate_nutrition_score(protein_per_100g)
        global_score = calculate_global_score(price_per_kg, protein_per_100g)

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
            "score_prix": price_score,
            "score_nutrition": nutrition_score,
            "score_global": global_score,
            "date_recuperation": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    except Exception as e:
        logger.warning(f"Error extracting data from {url}: {e}")
        return None


def scrape_products(api_key: str, progress_callback=None, status_callback=None) -> list[dict]:
    if status_callback:
        status_callback("Recherche de produits sur internet...")

    urls = search_all_queries(api_key, progress_callback)

    if status_callback:
        status_callback(f"{len(urls)} URLs de produits trouvées. Extraction des données...")

    products = []
    for i, url in enumerate(urls):
        if progress_callback:
            progress_callback(i, len(urls), f"Extraction: {urlparse(url).netloc}")

        product = extract_product_data(url)
        if product:
            products.append(product)

        time.sleep(REQUEST_DELAY)

    products.sort(key=lambda p: p.get("score_global") or -1, reverse=True)

    return products
