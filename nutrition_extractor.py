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
    re.compile(r"pour\s+100\s*grammes?", re.I),  # AMÉLIORATION
]

SERVING_HEADER_PATTERNS = [
    re.compile(r"(?:par|per|pour)\s+(?:dose|portion|serving|scoop)", re.I),
    re.compile(r"(?:dose|portion|serving|scoop)", re.I),
    re.compile(r"par\s+mesure", re.I),  # AMÉLIORATION
]

PROTEIN_LABEL_PATTERNS = [
    re.compile(r"prot[ée]ines?", re.I),
    re.compile(r"protein", re.I),
    re.compile(r"dont\s+prot", re.I),  # AMÉLIORATION: "dont protéines"
]

SERVING_SIZE_PATTERNS = [
    re.compile(r"(?:taille|dose|portion|serving\s*size|scoop)\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*(?:par\s+)?(?:dose|portion|serving|scoop)", re.I),
    re.compile(r"(?:dose|portion|serving)\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*(?:grammes|g)\b", re.I),
    re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:g|grammes)\s*\((?:1\s+)?(?:dose|scoop|mesure)\)", re.I),
    re.compile(r"(?:mesure|cuill[èe]re)\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),  # AMÉLIORATION
    re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*/\s*(?:dose|serving|scoop)", re.I),  # AMÉLIORATION
]

PROTEIN_PER_SERVING_PATTERNS = [
    re.compile(r"prot[ée]ines?\s*(?:par\s+(?:dose|portion|serving))?\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"protein\s*(?:per\s+serving)?\s*[:\-=]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
]

# AMÉLIORATION: patterns pour les calories
KCAL_PATTERNS = [
    re.compile(r"[ée]nergie\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*kcal", re.I),
    re.compile(r"calories?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*kcal", re.I),
    re.compile(r"(\d+(?:[.,]\d+)?)\s*kcal\s*(?:pour|per|/)\s*100\s*g", re.I),
    re.compile(r"valeur\s+[ée]nerg[ée]tique\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*kcal", re.I),
]

# AMÉLIORATION: patterns pour les glucides, lipides, sel
CARBS_PATTERNS = [
    re.compile(r"glucides?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"carbohydrates?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
]
FAT_PATTERNS = [
    re.compile(r"lipides?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"mati[èe]res?\s+grasses?\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"fat\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
]
SALT_PATTERNS = [
    re.compile(r"sel\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
    re.compile(r"sodium\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*(?:g|mg)", re.I),
    re.compile(r"salt\s*[:\|]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
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
        "kcal_per_100g": None,   # AMÉLIORATION
        "fat_per_100g": None,    # AMÉLIORATION
        "carbs_per_100g": None,  # AMÉLIORATION
        "salt_per_100g": None,   # AMÉLIORATION
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

            # AMÉLIORATION: extraire aussi calories, glucides, lipides, sel
            label_lower = label.lower()
            if any(k in label_lower for k in ["kcal", "calorie", "énergie", "energie"]):
                if per100g_col is not None and per100g_col < len(cells):
                    val = _parse_float(cells[per100g_col].get_text(strip=True))
                    if val and 200 <= val <= 700:
                        result["kcal_per_100g"] = val

            if any(k in label_lower for k in ["glucide", "carbohydr", "hydrate de carbone"]):
                if per100g_col is not None and per100g_col < len(cells):
                    val = _parse_float(cells[per100g_col].get_text(strip=True))
                    if val and 0 <= val <= 80:
                        result["carbs_per_100g"] = val

            if any(k in label_lower for k in ["lipide", "matière grasse", "graisse", "fat"]):
                if per100g_col is not None and per100g_col < len(cells):
                    val = _parse_float(cells[per100g_col].get_text(strip=True))
                    if val and 0 <= val <= 50:
                        result["fat_per_100g"] = val

            if any(k in label_lower for k in ["sel", "sodium", "salt"]):
                if per100g_col is not None and per100g_col < len(cells):
                    val = _parse_float(cells[per100g_col].get_text(strip=True))
                    if val is not None and 0 <= val <= 5:
                        result["salt_per_100g"] = val

            if not _is_protein_label(label):
                continue

            if per100g_col is not None and per100g_col < len(cells):
                val = _parse_float(cells[per100g_col].get_text(strip=True))
                # AMÉLIORATION: seuil minimal abaissé à 5g (certaines whey légères)
                if val and 5 < val <= 95:
                    result["protein_per_100g"] = val
                    result["source"] = "table_per100g"
                    logger.debug(f"[NUTRITION] Table per 100g protein: {val}g")

            if serving_col is not None and serving_col < len(cells):
                val = _parse_float(cells[serving_col].get_text(strip=True))
                if val and 3 < val <= 60:
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

    # AMÉLIORATION: chercher dans les tableaux HTML alternatifs (dl, section, etc.)
    div_tables = soup.find_all(["div", "section", "dl"],
                                class_=re.compile(r"nutri|valeur|nutrition-table|product-nutrition|macro|composition", re.I))
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
                    if val and 5 < val <= 95:
                        result["protein_per_100g"] = val
                        result["source"] = "div_table"
                        return result

    # AMÉLIORATION: chercher dans les listes de macros (format "Protéines : 74g")
    page_text = soup.get_text(" ", strip=True)
    for pat in [
        re.compile(r"prot[ée]ines?\s*[:\|]\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
        re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*de\s*prot[ée]ines?", re.I),
    ]:
        m = pat.search(page_text)
        if m:
            val = float(m.group(1).replace(",", "."))
            if 10 <= val <= 95 and not _is_marketing_context(page_text[max(0, m.start()-50):m.end()+50]):
                result["protein_per_100g"] = val
                result["source"] = "text_pattern"
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
            # AMÉLIORATION: plage élargie (certaines doses font 20g ou 100g)
            if 15 <= val <= 120:
                result["serving_size_g"] = val
                break

    if result["serving_size_g"]:
        for pattern in PROTEIN_PER_SERVING_PATTERNS:
            for m in pattern.finditer(page_text):
                val = float(m.group(1).replace(",", "."))
                if 3 <= val <= 80:
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
        # AMÉLIORATION: seuil minimal abaissé à 10g/100g
        if 10 <= calc <= 95:
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

    # AMÉLIORATION: seuil minimal abaissé à 10g (certains produits légitimes)
    if val < 10:
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

    # Priorité 1: tableau nutritionnel
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

    # Priorité 2: calcul via dose
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

    # Priorité 3: regex sur le texte
    page_text = soup.get_text(" ", strip=True)

    regex_patterns = [
        re.compile(r"prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*g\s*(?:pour|per|/)\s*100\s*g", re.I),
        re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s*(?:de\s+)?prot[ée]ines?\s*(?:pour|per|/)\s*100\s*g", re.I),
        re.compile(r"(?:pour|per)\s+100\s*g\s*[:\-]?\s*.*?prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
        re.compile(r"teneur\s+en\s+prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*(?:g|%)", re.I),
        # AMÉLIORATION: patterns supplémentaires
        re.compile(r"(\d+(?:[.,]\d+)?)\s*g\s+prot[ée]ines?\s+/\s*100\s*g", re.I),
        re.compile(r"prot[ée]ines?\s*\(100\s*g\)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)", re.I),
        re.compile(r"dont\s+prot[ée]ines?\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*g", re.I),
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


# ============================================================
# AMÉLIORATION: Nouvelle fonction pour extraire l'aminogramme
# ============================================================

AMINO_ACIDS = {
    "leucine":      [r"l[\-\s]?leucine", r"\bleucine\b"],
    "isoleucine":   [r"l[\-\s]?isoleucine", r"\bisoleucine\b"],
    "valine":       [r"l[\-\s]?valine", r"\bvaline\b"],
    "lysine":       [r"l[\-\s]?lysine", r"\blysine\b"],
    "methionine":   [r"l[\-\s]?m[ée]thionine", r"\bm[ée]thionine\b", r"methionine"],
    "phenylalanine":[r"l[\-\s]?ph[ée]nylalanine", r"ph[ée]nylalanine"],
    "threonine":    [r"l[\-\s]?thr[ée]onine", r"thr[ée]onine"],
    "tryptophan":   [r"l[\-\s]?tryptophane?", r"tryptophane?"],
    "histidine":    [r"l[\-\s]?histidine", r"\bhistidine\b"],
    "glutamine":    [r"l[\-\s]?glutamine", r"\bglutamine\b"],
    "arginine":     [r"l[\-\s]?arginine", r"\barginine\b"],
    "alanine":      [r"l[\-\s]?alanine", r"\balanine\b"],
    "glycine":      [r"l[\-\s]?glycine", r"\bglycine\b"],
    "proline":      [r"l[\-\s]?proline", r"\bproline\b"],
    "serine":       [r"l[\-\s]?s[ée]rine", r"s[ée]rine"],
    "tyrosine":     [r"l[\-\s]?tyrosine", r"\btyrosine\b"],
    "cysteine":     [r"l[\-\s]?cyst[ée]ine", r"cyst[ée]ine"],
    "aspartic_acid":[r"acide\s+aspartique", r"aspartate", r"aspartic"],
    "glutamic_acid":[r"acide\s+glutamique", r"glutamate", r"glutamic"],
    "asparagine":   [r"l[\-\s]?asparagine", r"\basparagine\b"],
}


def extract_aminogram_from_table(soup) -> dict:
    """
    AMÉLIORATION: Extraction avancée de l'aminogramme depuis les tableaux HTML.
    Détecte les colonnes per 100g / per serving et normalise les valeurs.
    """
    amino_result = {}

    tables = soup.find_all("table")
    for table in tables:
        table_text = table.get_text(" ", strip=True).lower()
        # Vérifier que c'est un tableau d'aminogramme
        amino_keywords = ["leucine", "isoleucine", "valine", "acide", "amino"]
        if not any(kw in table_text for kw in amino_keywords):
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
            label = cells[0].get_text(strip=True).lower()

            for amino_name, patterns in AMINO_ACIDS.items():
                if amino_name in amino_result:
                    continue
                if any(re.search(p, label, re.I) for p in patterns):
                    # Chercher la valeur dans la colonne per 100g en priorité
                    val = None
                    if per100g_col is not None and per100g_col < len(cells):
                        val = _parse_amino_value(cells[per100g_col].get_text(strip=True))
                    elif len(cells) > 1:
                        # Chercher dans toutes les cellules numériques
                        for cell in cells[1:]:
                            v = _parse_amino_value(cell.get_text(strip=True))
                            if v is not None:
                                val = v
                                break
                    if val is not None:
                        amino_result[amino_name] = val
                    break

    return amino_result


def extract_aminogram_from_text(text: str) -> dict:
    """
    AMÉLIORATION: Extraction de l'aminogramme depuis le texte brut.
    Gère les formats mg et g, avec et sans unité explicite.
    """
    amino_result = {}
    if not text:
        return amino_result

    t = text.lower()

    for amino_name, patterns in AMINO_ACIDS.items():
        for pat in patterns:
            # Format: "Leucine : 2.5g" ou "L-Leucine 2500mg"
            full_pattern = re.compile(
                pat + r"\s*[:\-\|]?\s*(\d+(?:[.,]\d+)?)\s*(mg|g)?",
                re.I
            )
            m = full_pattern.search(t)
            if m:
                val_str = m.group(1).replace(",", ".")
                unit = (m.group(2) or "g").lower()
                val = float(val_str)
                if unit == "mg":
                    val = val / 1000
                # Validation: entre 0.01g et 30g par 100g de protéines
                if 0.01 <= val <= 30:
                    amino_result[amino_name] = round(val, 2)
                    break

    return amino_result


def _parse_amino_value(text: str) -> float | None:
    """Parse une valeur d'acide aminé (g ou mg) et retourne en grammes."""
    if not text:
        return None
    text = text.strip()
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(mg|g)?", text, re.I)
    if not m:
        return None
    val = float(m.group(1).replace(",", "."))
    unit = (m.group(2) or "g").lower()
    if unit == "mg":
        val = val / 1000
    # Plage valide pour un acide aminé par 100g produit
    if 0.01 <= val <= 30:
        return round(val, 2)
    return None


def extract_full_aminogram(html) -> dict:
    """
    AMÉLIORATION: Fonction principale combinant toutes les sources
    pour extraire l'aminogramme complet.
    """
    if isinstance(html, str):
        soup = BeautifulSoup(html, "lxml")
    else:
        soup = html

    # Priorité 1: tableau HTML
    amino_from_table = extract_aminogram_from_table(soup)

    # Priorité 2: texte brut
    page_text = soup.get_text(" ", strip=True)
    amino_from_text = extract_aminogram_from_text(page_text)

    # Fusionner: le tableau prend priorité
    combined = {**amino_from_text, **amino_from_table}

    return combined
