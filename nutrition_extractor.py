import re
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MARKETING_BLACKLIST = [
    r"100\s*%\s*whey",
    r"100\s*%\s*prot[ée]ine",
    r"100\s*%\s*isolat",
    r"100\s*%\s*naturel",
    r"100\s*%\s*pure?",
    r"100\s*%\s*bio",
    r"100\s*%\s*vegan",
    r"100\s*%\s*grass",
]

MARKETING_BLACKLIST_RE = [re.compile(p, re.I) for p in MARKETING_BLACKLIST]

PER_100G_HEADER_PATTERNS = [
    re.compile(r"(?:pour|per|par)\s+100\s*g", re.I),
    re.compile(r"100\s*g", re.I),
    re.compile(r"/\s*100\s*g", re.I),
    re.compile(r"valeurs?\s+pour\s+100", re.I),
]

SERVING_HEADER_PATTERNS = [
    re.compile(r"(?:par|per|pour)\s+(?:dose|portion|serving|scoop)", re.I),
    re.compile(r"(?:dose|portion|serving|scoop)", re.I),
]

PROTEIN_LABEL_PATTERNS = [
    re.compile(r"prot[ée]ines?", re.I),
    re.compile(r"protein", re.I),
]

SERVING_SIZE_PATTERNS = [
    re.compile(r"(?:taille|dose|portion|serving\s*size|scoop)\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*(?:par\s+)?(?:dose|portion|serving|scoop)", re.I),
    re.compile(r"(?:dose|portion|serving)\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*(?:grammes|g)\b", re.I),
    re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:g|grammes)\s*\((?:1\s+)?(?:dose|scoop|mesure)\)", re.I),
]

PROTEIN_PER_SERVING_PATTERNS = [
    re.compile(r"prot[ée]ines?\s*(?:par\s+(?:dose|portion|serving))?\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"protein\s*(?:per\s+serving)?\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
]


def _parse_float(text: str) -> float | None:
    if not text:
        return None
    text = text.strip()
    m = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def _is_marketing_context(text: str) -> bool:
    return any(p.search(text) for p in MARKETING_BLACKLIST_RE)


def _find_per100g_col_index(header_cells) -> int | None:
    for i, cell in enumerate(header_cells):
        cell_text = cell.get_text(strip=True)
        for pattern in PER_100G_HEADER_PATTERNS:
            if pattern.search(cell_text):
                return i
    return None


def _find_serving_col_index(header_cells) -> int | None:
    for i, cell in enumerate(header_cells):
        cell_text = cell.get_text(strip=True)
        for pattern in SERVING_HEADER_PATTERNS:
            if pattern.search(cell_text):
                return i
    return None


def _is_protein_label(text: str) -> bool:
    return any(p.search(text) for p in PROTEIN_LABEL_PATTERNS)


def extract_nutrition_table(html) -> dict:
    result = {
        "protein_per_100g": None,
        "protein_per_serving": None,
        "serving_size_g": None,
        "source": None,
    }

    if isinstance(html, str):
        soup = BeautifulSoup(html, "lxml")
    else:
        soup = html

    tables = soup.find_all("table")
    for table in tables:
        table_text = table.get_text(" ", strip=True).lower()
        if not ("prot" in table_text and ("nutri" in table_text or "valeur" in table_text or "100" in table_text or "portion" in table_text)):
            continue

        rows = table.find_all("tr")
        if not rows:
            continue

        header_row = rows[0]
        header_cells = header_row.find_all(["td", "th"])

        per100g_col = _find_per100g_col_index(header_cells)
        serving_col = _find_serving_col_index(header_cells)

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True)
            if not _is_protein_label(label):
                continue

            if per100g_col is not None and per100g_col < len(cells):
                val = _parse_float(cells[per100g_col].get_text(strip=True))
                if val and 10 < val <= 95:
                    result["protein_per_100g"] = val
                    result["source"] = "table_per100g"
                    logger.debug(f"[NUTRITION] Table per 100g protein: {val}g")

            if serving_col is not None and serving_col < len(cells):
                val = _parse_float(cells[serving_col].get_text(strip=True))
                if val and 5 < val <= 60:
                    result["protein_per_serving"] = val

            if per100g_col is None and not result["protein_per_100g"]:
                for i in range(1, len(cells)):
                    cell_text = cells[i].get_text(strip=True)
                    val = _parse_float(cell_text)
                    if val and 15 <= val <= 95:
                        result["protein_per_100g"] = val
                        result["source"] = "table_inferred"
                        break

            if result["protein_per_100g"]:
                return result

    div_tables = soup.find_all(["div", "section", "dl"],
                                class_=re.compile(r"nutri|valeur|nutrition-table|product-nutrition", re.I))
    for div_table in div_tables:
        div_text = div_table.get_text(" ", strip=True).lower()
        if "prot" not in div_text:
            continue

        rows = div_table.find_all(["div", "dt", "dd", "li", "span", "p"])
        for i, el in enumerate(rows):
            el_text = el.get_text(strip=True)
            if _is_protein_label(el_text):
                for j in range(i + 1, min(i + 4, len(rows))):
                    val = _parse_float(rows[j].get_text(strip=True))
                    if val and 10 < val <= 95:
                        result["protein_per_100g"] = val
                        result["source"] = "div_table"
                        return result

    return result


def extract_serving_info(html) -> dict:
    result = {
        "serving_size_g": None,
        "protein_per_serving_g": None,
        "calculated_protein_per_100g": None,
    }

    if isinstance(html, str):
        soup = BeautifulSoup(html, "lxml")
    else:
        soup = html

    page_text = soup.get_text(" ", strip=True)

    for pattern in SERVING_SIZE_PATTERNS:
        m = pattern.search(page_text)
        if m:
            val = float(m.group(1).replace(",", "."))
            if 10 <= val <= 100:
                result["serving_size_g"] = val
                break

    if result["serving_size_g"]:
        for pattern in PROTEIN_PER_SERVING_PATTERNS:
            for m in pattern.finditer(page_text):
                val = float(m.group(1).replace(",", "."))
                if 5 <= val <= 60:
                    start = max(0, m.start() - 200)
                    context = page_text[start:m.end() + 100].lower()
                    if "100" in context and ("g" in context):
                        if any(p.search(context) for p in PER_100G_HEADER_PATTERNS):
                            continue
                    result["protein_per_serving_g"] = val
                    break
            if result["protein_per_serving_g"]:
                break

    if result["serving_size_g"] and result["protein_per_serving_g"]:
        calc = (result["protein_per_serving_g"] / result["serving_size_g"]) * 100
        calc = round(calc, 1)
        if 15 <= calc <= 95:
            result["calculated_protein_per_100g"] = calc
            logger.debug(
                f"[NUTRITION] Serving calc: {result['protein_per_serving_g']}g / "
                f"{result['serving_size_g']}g = {calc}g/100g"
            )

    return result


def validate_protein_value(val: float | None) -> tuple[float | None, bool]:
    if val is None:
        return None, False

    if val == 100 or val >= 96:
        logger.warning(f"[NUTRITION] Suspect protein value: {val}g/100g => rejected")
        return None, True

    if val < 15:
        logger.warning(f"[NUTRITION] Too low protein value for whey: {val}g/100g => rejected")
        return None, False

    return val, False


def extract_protein_per_100g(html) -> dict:
    result = {
        "protein_per_100g": None,
        "protein_source": None,
        "protein_confidence": 0.0,
        "protein_suspect": False,
        "serving_size_g": None,
        "protein_per_serving_g": None,
    }

    if isinstance(html, str):
        soup = BeautifulSoup(html, "lxml")
    else:
        soup = html

    table_data = extract_nutrition_table(soup)
    if table_data["protein_per_100g"]:
        val, suspect = validate_protein_value(table_data["protein_per_100g"])
        if suspect:
            result["protein_suspect"] = True
            logger.warning(f"[NUTRITION] Table value suspect: {table_data['protein_per_100g']}")
        if val:
            result["protein_per_100g"] = val
            result["protein_source"] = "table"
            result["protein_confidence"] = 0.9
            return result

    serving_data = extract_serving_info(soup)
    if serving_data["calculated_protein_per_100g"]:
        val, suspect = validate_protein_value(serving_data["calculated_protein_per_100g"])
        if suspect:
            result["protein_suspect"] = True
        if val:
            result["protein_per_100g"] = val
            result["protein_source"] = "serving_calc"
            result["protein_confidence"] = 0.7
            result["serving_size_g"] = serving_data["serving_size_g"]
            result["protein_per_serving_g"] = serving_data["protein_per_serving_g"]
            return result

    page_text = soup.get_text(" ", strip=True)

    regex_patterns = [
        re.compile(r"prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*g\s*(?:pour|per|/)\s*100\s*g", re.I),
        re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*(?:de\s+)?prot[ée]ines?\s*(?:pour|per|/)\s*100\s*g", re.I),
        re.compile(r"(?:pour|per)\s+100\s*g\s*[:\-]?\s*.*?prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
        re.compile(r"teneur\s+en\s+prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:g|%)", re.I),
    ]

    for pat in regex_patterns:
        m = pat.search(page_text)
        if m:
            raw_val = float(m.group(1).replace(",", "."))

            context_start = max(0, m.start() - 100)
            context = page_text[context_start:m.end() + 50]
            if _is_marketing_context(context):
                logger.debug(f"[NUTRITION] Regex match {raw_val} rejected: marketing context")
                continue

            val, suspect = validate_protein_value(raw_val)
            if suspect:
                result["protein_suspect"] = True
            if val:
                result["protein_per_100g"] = val
                result["protein_source"] = "regex"
                result["protein_confidence"] = 0.5
                return result

    return result


def extract_protein_from_jsonld(jsonld: dict | None) -> dict:
    result = {
        "protein_per_100g": None,
        "protein_source": None,
        "protein_confidence": 0.0,
        "protein_suspect": False,
    }

    if not jsonld:
        return result

    nutrition = jsonld.get("nutrition", {})
    if not isinstance(nutrition, dict):
        return result

    protein_content = nutrition.get("proteinContent", "")
    if not protein_content:
        return result

    val = _parse_float(str(protein_content))
    if val is None:
        return result

    validated, suspect = validate_protein_value(val)
    if suspect:
        result["protein_suspect"] = True
    if validated:
        result["protein_per_100g"] = validated
        result["protein_source"] = "jsonld"
        result["protein_confidence"] = 0.85
    return result
