"""
Multi-source nutrition extractor with confidence-based fusion and OCR fallback.

Pipeline:
  Source A - Structured data (JSON-LD, OpenGraph, microdata) → high confidence
  Source B - HTML tables/sections (nutrition tables, amino tables) → high confidence
  Source C - OCR on product images (nutrition labels) → medium confidence, only triggered if needed

Each extracted value carries: value, unit, source, confidence, raw_snippet
Fusion picks the highest-confidence source per field.
"""

import re
import os
import logging
import httpx
from typing import Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


NUTRITION_FIELDS = [
    "protein_per_100g", "carbs_per_100g", "sugar_per_100g",
    "fat_per_100g", "sat_fat_per_100g", "kcal_per_100g",
    "salt_per_100g", "fiber_per_100g",
]

AMINO_FIELDS = [
    "leucine", "isoleucine", "valine",
    "glutamine", "arginine", "lysine",
    "methionine", "phenylalanine", "threonine",
    "tryptophan", "histidine", "alanine",
    "glycine", "proline", "serine",
    "tyrosine", "aspartic_acid", "cysteine",
]

FIELD_ALIASES = {
    "protéines": "protein_per_100g", "proteines": "protein_per_100g",
    "protein": "protein_per_100g", "proteins": "protein_per_100g",
    "protéine": "protein_per_100g",
    "glucides": "carbs_per_100g", "carbohydrates": "carbs_per_100g",
    "carbohydrate": "carbs_per_100g", "glucide": "carbs_per_100g",
    "carbs": "carbs_per_100g",
    "sucres": "sugar_per_100g", "sugars": "sugar_per_100g",
    "sugar": "sugar_per_100g", "dont sucres": "sugar_per_100g",
    "of which sugars": "sugar_per_100g",
    "matières grasses": "fat_per_100g", "matieres grasses": "fat_per_100g",
    "lipides": "fat_per_100g", "fat": "fat_per_100g", "fats": "fat_per_100g",
    "total fat": "fat_per_100g", "graisses": "fat_per_100g",
    "acides gras saturés": "sat_fat_per_100g", "saturated fat": "sat_fat_per_100g",
    "dont acides gras saturés": "sat_fat_per_100g",
    "dont acides gras satures": "sat_fat_per_100g",
    "saturated": "sat_fat_per_100g", "ag saturés": "sat_fat_per_100g",
    "énergie": "kcal_per_100g", "energie": "kcal_per_100g",
    "energy": "kcal_per_100g", "calories": "kcal_per_100g",
    "valeur énergétique": "kcal_per_100g", "valeur energetique": "kcal_per_100g",
    "kcal": "kcal_per_100g",
    "sel": "salt_per_100g", "salt": "salt_per_100g", "sodium": "salt_per_100g",
    "fibres": "fiber_per_100g", "fiber": "fiber_per_100g",
    "fibre alimentaire": "fiber_per_100g", "dietary fiber": "fiber_per_100g",
}

AMINO_ALIASES = {
    "leucine": "leucine", "l-leucine": "leucine",
    "isoleucine": "isoleucine", "l-isoleucine": "isoleucine",
    "valine": "valine", "l-valine": "valine",
    "glutamine": "glutamine", "l-glutamine": "glutamine",
    "acide glutamique": "glutamine", "glutamic acid": "glutamine",
    "ac. glutamique": "glutamine", "ac glutamique": "glutamine",
    "glutamique + glutamine": "glutamine", "glutamique": "glutamine",
    "arginine": "arginine", "l-arginine": "arginine",
    "lysine": "lysine", "l-lysine": "lysine",
    "méthionine": "methionine", "methionine": "methionine", "l-methionine": "methionine",
    "phénylalanine": "phenylalanine", "phenylalanine": "phenylalanine",
    "l-phenylalanine": "phenylalanine", "phenylalanine + tyrosine": "phenylalanine",
    "thréonine": "threonine", "threonine": "threonine", "l-threonine": "threonine",
    "tryptophane": "tryptophan", "tryptophan": "tryptophan", "l-tryptophan": "tryptophan",
    "histidine": "histidine", "l-histidine": "histidine",
    "alanine": "alanine", "l-alanine": "alanine",
    "glycine": "glycine",
    "proline": "proline", "l-proline": "proline",
    "sérine": "serine", "serine": "serine", "l-serine": "serine",
    "tyrosine": "tyrosine", "l-tyrosine": "tyrosine",
    "acide aspartique": "aspartic_acid", "aspartic acid": "aspartic_acid",
    "aspartique + asparagine": "aspartic_acid", "aspartique": "aspartic_acid",
    "ac. aspartique": "aspartic_acid", "ac aspartique": "aspartic_acid",
    "cystéine": "cysteine", "cysteine": "cysteine", "l-cysteine": "cysteine",
    "cystine": "cysteine", "cysteine + methionine": "cysteine",
    "methionine + cysteine": "cysteine",
}


class NutritionEvidence:
    """A single extracted value with provenance."""
    def __init__(self, field: str, value: float, unit: str, source: str,
                 confidence: float, raw_snippet: str = "", amino_base: str = ""):
        self.field = field
        self.value = value
        self.unit = unit
        self.source = source
        self.confidence = confidence
        self.raw_snippet = raw_snippet[:200] if raw_snippet else ""
        self.amino_base = amino_base

    def to_dict(self):
        return {
            "field": self.field,
            "value": self.value,
            "unit": self.unit,
            "source": self.source,
            "confidence": self.confidence,
            "raw_snippet": self.raw_snippet,
            "amino_base": self.amino_base,
        }

    def __repr__(self):
        return f"Evidence({self.field}={self.value}{self.unit} src={self.source} conf={self.confidence})"


def _parse_num(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().replace(",", ".").replace(" ", "")
    m = re.search(r"(\d+\.?\d*)", text)
    if m:
        return float(m.group(1))
    return None


def _detect_unit(text: str) -> str:
    text = text.lower().strip()
    if "kcal" in text:
        return "kcal"
    if "kj" in text:
        return "kj"
    if "mg" in text:
        return "mg"
    if "µg" in text or "mcg" in text:
        return "µg"
    return "g"


def _normalize_value(value: float, unit: str) -> float:
    if unit == "mg":
        return value / 1000.0
    if unit == "kj":
        return round(value / 4.184, 1)
    if unit == "µg":
        return value / 1_000_000.0
    return value


def _match_field(label: str) -> tuple[str | None, str]:
    label_clean = re.sub(r'[^a-zàâäéèêëïîôùûüç0-9\s\-/]', '', label.lower().strip())
    label_clean = re.sub(r'\s+', ' ', label_clean).strip()

    best_match = None
    best_len = 0
    best_type = ""

    for alias, field in FIELD_ALIASES.items():
        if alias in label_clean and len(alias) > best_len:
            best_match = field
            best_len = len(alias)
            best_type = "nutrition"
    for alias, field in AMINO_ALIASES.items():
        if alias in label_clean and len(alias) > best_len:
            best_match = field
            best_len = len(alias)
            best_type = "amino"

    if best_match:
        return best_match, best_type
    return None, ""


def _detect_amino_base(header_text: str) -> str:
    t = header_text.lower()
    if re.search(r"(pour|per|par)\s+100\s*g\s+(de\s+prot[eé]ine|d.acides?\s+amin[eé])", t):
        return "per_100g_protein"
    if re.search(r"100\s*g\s+(de\s+prot[eé]ine|d.acides?\s+amin[eé])", t):
        return "per_100g_protein"
    if re.search(r"(pour|per|par)\s+100\s*g(\s+de\s+produit)?", t):
        return "per_100g"
    if re.search(r"(pour|per|par)\s+(dose|portion|serving|scoop)", t):
        return "per_serving"
    return "unknown"


# ──────────────────────────────────────────────
# HELPER: Deep JSON traversal for nutrition data
# ──────────────────────────────────────────────

def _deep_find_nutrition_in_json(obj, depth=0, max_depth=8) -> list[NutritionEvidence]:
    if depth > max_depth:
        return []
    evidences = []

    if isinstance(obj, dict):
        for key, val in obj.items():
            key_lower = key.lower()

            field, ftype = _match_field(key_lower)
            if field and isinstance(val, (int, float, str)):
                num_val = _parse_num(str(val)) if isinstance(val, str) else float(val)
                if num_val is not None:
                    unit = _detect_unit(str(val)) if isinstance(val, str) else "g"
                    if field == "kcal_per_100g" and unit == "g":
                        unit = "kcal"
                    normalized = _normalize_value(num_val, unit)
                    if ftype == "amino":
                        if 0.01 <= normalized <= 50:
                            evidences.append(NutritionEvidence(
                                field=field, value=normalized,
                                unit="g", source="json_script",
                                confidence=0.75,
                                raw_snippet=f"{key}: {val}",
                                amino_base="unknown",
                            ))
                    elif ftype == "nutrition":
                        if field == "kcal_per_100g":
                            if 50 <= normalized <= 800:
                                evidences.append(NutritionEvidence(
                                    field=field, value=normalized,
                                    unit="kcal", source="json_script",
                                    confidence=0.75,
                                    raw_snippet=f"{key}: {val}",
                                ))
                        elif 0 <= normalized <= 100:
                            evidences.append(NutritionEvidence(
                                field=field, value=normalized,
                                unit="g", source="json_script",
                                confidence=0.75,
                                raw_snippet=f"{key}: {val}",
                            ))

            if isinstance(val, (dict, list)):
                evidences.extend(_deep_find_nutrition_in_json(val, depth + 1, max_depth))

    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                evidences.extend(_deep_find_nutrition_in_json(item, depth + 1, max_depth))

    return evidences


def _extract_from_script_json(soup: BeautifulSoup) -> list[NutritionEvidence]:
    import json
    evidences = []

    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data and next_data.string:
        try:
            data = json.loads(next_data.string)
            evidences.extend(_deep_find_nutrition_in_json(data))
        except (json.JSONDecodeError, Exception):
            pass

    for script in soup.find_all("script"):
        if not script.string:
            continue
        s = script.string.strip()

        for pattern_name, regex in [
            ("nuxt", r"window\.__NUXT__\s*=\s*(\{.+?\})\s*;"),
            ("initial_state", r"window\.__INITIAL_STATE__\s*=\s*(\{.+?\})\s*;"),
        ]:
            m = re.search(regex, s, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    evidences.extend(_deep_find_nutrition_in_json(data))
                except (json.JSONDecodeError, Exception):
                    pass

        if script.get("type") == "application/json" and script.get("id") != "__NEXT_DATA__":
            try:
                data = json.loads(s)
                if isinstance(data, (dict, list)):
                    evidences.extend(_deep_find_nutrition_in_json(data))
            except (json.JSONDecodeError, Exception):
                pass

        if "application/ld+json" not in (script.get("type") or ""):
            amino_kw = ["leucine", "isoleucine", "valine", "glutamine", "arginine",
                        "lysine", "bcaa", "aminogram"]
            s_lower = s[:5000].lower()
            if any(kw in s_lower for kw in amino_kw):
                json_objects = re.findall(r'\{[^{}]{20,}\}', s[:10000])
                for jo in json_objects[:5]:
                    try:
                        data = json.loads(jo)
                        ev = _deep_find_nutrition_in_json(data, max_depth=3)
                        if ev:
                            evidences.extend(ev)
                    except (json.JSONDecodeError, Exception):
                        pass

    return evidences


# ──────────────────────────────────────────────
# SOURCE A: Structured data (JSON-LD, OG, microdata)
# ──────────────────────────────────────────────

def extract_source_a(jsonld: dict | None, og: dict, soup: BeautifulSoup) -> list[NutritionEvidence]:
    evidences = []

    if jsonld:
        nutrition = jsonld.get("nutrition", {})
        if isinstance(nutrition, dict):
            mapping = {
                "proteinContent": "protein_per_100g",
                "carbohydrateContent": "carbs_per_100g",
                "sugarContent": "sugar_per_100g",
                "fatContent": "fat_per_100g",
                "saturatedFatContent": "sat_fat_per_100g",
                "calories": "kcal_per_100g",
                "sodiumContent": "salt_per_100g",
                "fiberContent": "fiber_per_100g",
            }
            for jld_key, field in mapping.items():
                raw = nutrition.get(jld_key, "")
                if raw:
                    val = _parse_num(str(raw))
                    unit = _detect_unit(str(raw))
                    if val is not None:
                        evidences.append(NutritionEvidence(
                            field=field, value=_normalize_value(val, unit),
                            unit="g" if field != "kcal_per_100g" else "kcal",
                            source="jsonld_nutrition",
                            confidence=0.9,
                            raw_snippet=f"{jld_key}: {raw}",
                        ))

        addl_props = jsonld.get("additionalProperty", [])
        if isinstance(addl_props, list):
            for prop in addl_props:
                if not isinstance(prop, dict):
                    continue
                name = prop.get("name", "")
                value_raw = prop.get("value", "")
                field, ftype = _match_field(name)
                if field and value_raw:
                    val = _parse_num(str(value_raw))
                    unit = _detect_unit(str(value_raw))
                    if val is not None:
                        evidences.append(NutritionEvidence(
                            field=field, value=_normalize_value(val, unit),
                            unit="g" if ftype == "nutrition" else unit,
                            source="jsonld_additionalProperty",
                            confidence=0.85,
                            raw_snippet=f"{name}: {value_raw}",
                        ))

    return evidences


# ──────────────────────────────────────────────
# SOURCE B: HTML tables and sections
# ──────────────────────────────────────────────

def _extract_from_table(table, soup_url: str = "") -> list[NutritionEvidence]:
    evidences = []
    rows = table.find_all("tr")
    if not rows:
        return evidences

    all_header_texts = []
    for r in rows[:3]:
        cells = r.find_all(["td", "th"])
        all_header_texts.append(" ".join(c.get_text(strip=True) for c in cells).lower())
    full_header_text = " ".join(all_header_texts)

    header_cells = rows[0].find_all(["td", "th"])
    header_text = " ".join(c.get_text(strip=True) for c in header_cells).lower()

    per100g_prot_col = None
    per100g_col = None
    serving_col = None
    for i, cell in enumerate(header_cells):
        ct = cell.get_text(strip=True).lower()
        if re.search(r"(pour|per|par)\s+100\s*g\s+(de\s+prot[eé]ine|d.acides?\s+amin[eé])", ct):
            per100g_prot_col = i
        elif re.search(r"(pour|per|par)\s+100\s*g", ct) or ct == "100 g" or ct == "100g":
            if per100g_col is None:
                per100g_col = i
        elif re.search(r"(dose|portion|serving|scoop)", ct):
            serving_col = i
        elif re.search(r"(pour|per|par)\s+\d+[\.,]?\d*\s*g(?!\s*(de\s+prot|d.acide))", ct) and per100g_col is None:
            serving_col = i

    context_text = full_header_text
    for sibling_list in [table.previous_siblings, table.next_siblings]:
        for sib in sibling_list:
            if hasattr(sib, 'get_text'):
                st = sib.get_text(strip=True).lower()
                if st and len(st) < 200:
                    context_text = st + " " + context_text
                    break
            elif isinstance(sib, str) and sib.strip():
                context_text = sib.strip().lower() + " " + context_text
                break
    if table.parent:
        parent_heading = table.parent.find(["h2", "h3", "h4", "h5", "p", "div"],
                                            string=re.compile(r"(?i)(100\s*g|amino|portion|dose)"))
        if parent_heading:
            context_text = parent_heading.get_text(strip=True).lower() + " " + context_text

    amino_base = _detect_amino_base(context_text)
    is_amino_table = any(kw in context_text for kw in ["amino", "acide amin", "bcaa", "aminogramme"])

    if is_amino_table and amino_base == "unknown":
        if per100g_prot_col is not None:
            amino_base = "per_100g_protein"
        elif per100g_col is not None:
            amino_base = "per_100g"

    serving_size_g = None
    only_serving = serving_col is not None and per100g_col is None and per100g_prot_col is None
    if only_serving:
        for cell in header_cells:
            ct = cell.get_text(strip=True).lower()
            m = re.search(r"(?:pour|per|par|valeurs?\s+pour)\s+(\d+[\.,]?\d*)\s*g", ct)
            if m:
                sv = _parse_num(m.group(1))
                if sv and sv != 100:
                    serving_size_g = sv
                    break
        if not serving_size_g:
            m = re.search(r"(\d+)\s*g\s*\(", header_text)
            if m:
                sv = _parse_num(m.group(1))
                if sv and sv != 100:
                    serving_size_g = sv

    seen_fields = set()
    row_list = rows[1:]

    for row_idx, row in enumerate(row_list):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            if len(cells) == 1:
                label = cells[0].get_text(strip=True)
                field, ftype = _match_field(label)
                if field and field not in seen_fields and row_idx + 1 < len(row_list):
                    next_cells = row_list[row_idx + 1].find_all(["td", "th"])
                    if next_cells:
                        for nc in next_cells:
                            nc_text = nc.get_text(strip=True)
                            nc_val = _parse_num(nc_text)
                            if nc_val is not None:
                                cells = next_cells
                                break
                if len(cells) < 2:
                    continue
            else:
                continue

        label = cells[0].get_text(strip=True)
        field, ftype = _match_field(label)

        if field and ftype == "nutrition" and len(cells) >= 2:
            all_vals_none = all(_parse_num(cells[c].get_text(strip=True)) is None for c in range(1, len(cells)))
            if all_vals_none and row_idx + 1 < len(row_list):
                next_cells = row_list[row_idx + 1].find_all(["td", "th"])
                if len(next_cells) >= 2:
                    next_label = next_cells[0].get_text(strip=True).lower()
                    if re.match(r'\(n[=\s]', next_label) or not _match_field(next_label)[0]:
                        cells = next_cells

        if not field:
            label = cells[0].get_text(strip=True)
            field, ftype = _match_field(label)
        if not field:
            continue

        if field in seen_fields:
            continue

        if ftype == "amino" and per100g_prot_col is not None and per100g_prot_col < len(cells):
            value_col = per100g_prot_col
        elif per100g_col is not None:
            value_col = per100g_col
        else:
            value_col = 1
        if value_col >= len(cells):
            value_col = len(cells) - 1

        cell_text = cells[value_col].get_text(strip=True)
        val = _parse_num(cell_text)
        unit = _detect_unit(cell_text)

        if val is None:
            for ci in range(len(cells) - 1, 0, -1):
                alt_text = cells[ci].get_text(strip=True)
                alt_val = _parse_num(alt_text)
                if alt_val is not None:
                    cell_text = alt_text
                    val = alt_val
                    unit = _detect_unit(alt_text)
                    break

        if val is None:
            continue

        normalized = _normalize_value(val, unit)

        if only_serving and serving_size_g and serving_size_g > 0 and ftype == "nutrition":
            normalized = round(normalized * 100.0 / serving_size_g, 2)

        if ftype == "amino" and (normalized < 0.01 or normalized > 50):
            continue
        if ftype == "nutrition":
            if field == "kcal_per_100g" and (normalized < 50 or normalized > 800):
                continue
            elif field != "kcal_per_100g" and (normalized < 0 or normalized > 100):
                continue

        conf = 0.9 if per100g_col is not None else 0.7
        if only_serving and serving_size_g:
            conf = 0.65
        if ftype == "amino":
            if per100g_prot_col is not None or per100g_col is not None or is_amino_table:
                conf = 0.85
            else:
                conf = 0.75

        seen_fields.add(field)

        evidences.append(NutritionEvidence(
            field=field, value=normalized,
            unit="g" if unit in ("g", "mg", "µg") else unit,
            source="html_table",
            confidence=conf,
            raw_snippet=f"{label}: {cell_text}",
            amino_base=amino_base if ftype == "amino" else "",
        ))

    return evidences


def _extract_from_div_sections(soup: BeautifulSoup) -> list[NutritionEvidence]:
    evidences = []
    seen_fields = set()
    sections = soup.find_all(["div", "section", "dl"],
                              class_=re.compile(r"nutri|valeur|composition|amino|ingredi|nutrition", re.I))

    for section in sections:
        section_text = section.get_text(" ", strip=True).lower()
        amino_base = _detect_amino_base(section_text)

        items = section.find_all(["div", "dt", "dd", "li", "span", "p"])
        for i, el in enumerate(items):
            el_text = el.get_text(strip=True)
            if len(el_text) > 80:
                continue
            field, ftype = _match_field(el_text)
            if not field:
                continue
            if field in seen_fields:
                continue

            for j in range(i + 1, min(i + 4, len(items))):
                next_text = items[j].get_text(strip=True)
                if len(next_text) > 30:
                    continue
                val = _parse_num(next_text)
                if val is not None:
                    unit = _detect_unit(next_text)
                    normalized = _normalize_value(val, unit)

                    if ftype == "amino" and (normalized < 0.01 or normalized > 50):
                        continue
                    if ftype == "nutrition":
                        if field == "kcal_per_100g" and (normalized < 50 or normalized > 800):
                            continue
                        elif field != "kcal_per_100g" and normalized > 100:
                            continue

                    seen_fields.add(field)
                    evidences.append(NutritionEvidence(
                        field=field, value=normalized,
                        unit="g" if unit in ("g", "mg", "µg") else unit,
                        source="html_section",
                        confidence=0.65,
                        raw_snippet=f"{el_text}: {next_text}",
                        amino_base=amino_base if ftype == "amino" else "",
                    ))
                    break

    return evidences


def _extract_amino_from_text(text: str) -> list[NutritionEvidence]:
    evidences = []
    t = text.lower()

    amino_regex_map = {
        "leucine": [
            r"(?:l[\-\s]?)?leucine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "isoleucine": [
            r"(?:l[\-\s]?)?isoleucine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "valine": [
            r"(?:l[\-\s]?)?valine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "glutamine": [
            r"(?:l[\-\s]?)?glutamine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
            r"acide\s+glutamique\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "arginine": [
            r"(?:l[\-\s]?)?arginine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "lysine": [
            r"(?:l[\-\s]?)?lysine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "methionine": [
            r"(?:l[\-\s]?)?m[ée]thionine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "phenylalanine": [
            r"(?:l[\-\s]?)?ph[ée]nylalanine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "threonine": [
            r"(?:l[\-\s]?)?thr[ée]onine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "tryptophan": [
            r"(?:l[\-\s]?)?tryptophane?\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "histidine": [
            r"(?:l[\-\s]?)?histidine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "alanine": [
            r"(?:l[\-\s]?)?alanine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "glycine": [
            r"glycine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "proline": [
            r"(?:l[\-\s]?)?proline\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "serine": [
            r"(?:l[\-\s]?)?s[ée]rine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "tyrosine": [
            r"(?:l[\-\s]?)?tyrosine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "aspartic_acid": [
            r"acide\s+aspartique\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
            r"aspartic\s+acid\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
        "cysteine": [
            r"(?:l[\-\s]?)?cyst[ée]ine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
            r"cystine\s*[:\s\|]*\s*(\d+(?:[.,]\d+)?)\s*(mg|g)",
        ],
    }

    amino_base = "unknown"
    if re.search(r"(pour|per|par)\s+100\s*g\s+de\s+prot[eé]ine", t):
        amino_base = "per_100g_protein"
    elif re.search(r"aminogramme.{0,30}(pour|per|par)\s+100\s*g", t):
        amino_base = "per_100g"
    elif re.search(r"(pour|per|par)\s+(dose|portion|serving|scoop)", t):
        amino_base = "per_serving"

    for amino_name, patterns in amino_regex_map.items():
        for pat in patterns:
            m = re.search(pat, t)
            if m:
                val = float(m.group(1).replace(",", "."))
                unit = m.group(2)
                normalized = _normalize_value(val, unit)
                if 0.01 <= normalized <= 50:
                    evidences.append(NutritionEvidence(
                        field=amino_name, value=normalized,
                        unit="g", source="regex_text",
                        confidence=0.65,
                        raw_snippet=m.group(0)[:100],
                        amino_base=amino_base,
                    ))
                break

    return evidences


def _extract_nutrition_from_text(text: str) -> list[NutritionEvidence]:
    evidences = []
    t = text.lower()

    nutrition_regex_map = {
        "protein_per_100g": [
            r"prot[ée]ines?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
            r"protein\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
        ],
        "carbs_per_100g": [
            r"glucides?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
            r"carbohydrates?\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
        ],
        "fat_per_100g": [
            r"(?:mati[èe]res?\s+grasses?|lipides?|graisses?)\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
            r"(?:total\s+)?fat\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
        ],
        "kcal_per_100g": [
            r"(\d+(?:[.,]\d+)?)\s*kcal",
            r"[ée]nergie\s*[:\s]*(\d+(?:[.,]\d+)?)\s*kcal",
            r"calories?\s*[:\s]*(\d+(?:[.,]\d+)?)",
        ],
        "salt_per_100g": [
            r"sel\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
            r"salt\s*[:\s]*(\d+(?:[.,]\d+)?)\s*(g|mg)",
        ],
    }

    for field, patterns in nutrition_regex_map.items():
        for pat in patterns:
            m = re.search(pat, t)
            if m:
                val = float(m.group(1).replace(",", "."))
                if field == "kcal_per_100g":
                    if 50 <= val <= 800:
                        evidences.append(NutritionEvidence(
                            field=field, value=val, unit="kcal",
                            source="regex_text", confidence=0.55,
                            raw_snippet=m.group(0)[:100],
                        ))
                else:
                    unit = m.group(2) if len(m.groups()) > 1 else "g"
                    normalized = _normalize_value(val, unit)
                    if 0 <= normalized <= 100:
                        evidences.append(NutritionEvidence(
                            field=field, value=normalized, unit="g",
                            source="regex_text", confidence=0.55,
                            raw_snippet=m.group(0)[:100],
                        ))
                break

    return evidences


def extract_source_b(soup: BeautifulSoup) -> list[NutritionEvidence]:
    evidences = []

    for table in soup.find_all("table"):
        table_text = table.get_text(" ", strip=True).lower()
        keywords = ["prot", "nutri", "valeur", "amino", "bcaa", "leucine",
                     "carb", "fat", "lipid", "glucid", "calori", "énergie"]
        if not any(kw in table_text for kw in keywords):
            continue
        evidences.extend(_extract_from_table(table))

    evidences.extend(_extract_from_div_sections(soup))

    script_evidences = _extract_from_script_json(soup)
    if script_evidences:
        existing_fields = {e.field for e in evidences}
        for ev in script_evidences:
            if ev.field not in existing_fields:
                evidences.append(ev)
                existing_fields.add(ev.field)
        logger.info(f"[MULTI] Script JSON extraction: {len(script_evidences)} values found")

    page_text = soup.get_text(" ", strip=True)
    amino_fields_found = {e.field for e in evidences if e.field in AMINO_FIELDS}
    if len(amino_fields_found) < 3:
        text_amino = _extract_amino_from_text(page_text)
        for ev in text_amino:
            if ev.field not in amino_fields_found:
                evidences.append(ev)
                amino_fields_found.add(ev.field)

    nutrition_fields_found = {e.field for e in evidences if e.field in NUTRITION_FIELDS}
    if len(nutrition_fields_found) < 3:
        text_nutri = _extract_nutrition_from_text(page_text)
        for ev in text_nutri:
            if ev.field not in nutrition_fields_found:
                evidences.append(ev)
                nutrition_fields_found.add(ev.field)

    return evidences


# ──────────────────────────────────────────────
# SOURCE C: OCR / Vision on product images
# ──────────────────────────────────────────────

def find_nutrition_images(soup: BeautifulSoup, page_url: str) -> list[str]:
    candidates = []
    product_images = []

    for img in soup.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "") or img.get("data-lazy-src", "")
        if not src:
            continue
        if any(skip in src.lower() for skip in ["icon", "logo", "avatar", "flag", "badge", "pixel", ".svg", "tracking", "analytics"]):
            continue

        alt = (img.get("alt", "") or "").lower()
        title = (img.get("title", "") or "").lower()
        cls = " ".join(img.get("class", [])).lower()
        parent_text = (img.parent.get_text(strip=True) if img.parent else "").lower()[:100]
        src_lower = src.lower()

        score = 0
        nutrition_kw = ["nutri", "valeur", "composition", "amino", "etiquette",
                        "label", "ingrédient", "ingredient", "tableau", "information",
                        "nutrition-facts", "back", "dos", "verso", "detail"]
        for kw in nutrition_kw:
            if kw in alt or kw in title or kw in cls or kw in parent_text or kw in src_lower:
                score += 2

        width = img.get("width", "")
        height = img.get("height", "")
        w = _parse_num(str(width)) or 0
        h = _parse_num(str(height)) or 0

        if w >= 400 or h >= 400:
            score += 1
        if w >= 200 or h >= 200:
            is_product_img = any(kw in cls or kw in alt for kw in ["product", "produit", "gallery", "main", "zoom", "primary"])
            if is_product_img:
                score += 1

        full_url = urljoin(page_url, src)
        if not full_url.startswith("http"):
            continue

        if score > 0:
            candidates.append((score, full_url))
        elif (w >= 300 or h >= 300) and any(ext in src_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            product_images.append(full_url)

    candidates.sort(key=lambda x: -x[0])
    result = [url for _, url in candidates[:3]]

    if len(result) < 2 and product_images:
        for pi in product_images[:2]:
            if pi not in result:
                result.append(pi)
            if len(result) >= 3:
                break

    return result


def _pick_highest_res(image_url: str) -> str:
    for size in ["1200", "1000", "800"]:
        for param in [f"?width={size}", f"?w={size}", f"&width={size}"]:
            if "?" not in image_url:
                return image_url + param
    return image_url


def ocr_nutrition_image(image_url: str) -> list[NutritionEvidence]:
    evidences = []

    base_url = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
    api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")

    if not base_url or not api_key:
        logger.warning("[OCR] OpenAI integration not configured, skipping OCR")
        return evidences

    try:
        from openai import OpenAI
        client = OpenAI(base_url=base_url, api_key=api_key)

        hd_url = _pick_highest_res(image_url)

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Tu es un expert en extraction de données nutritionnelles depuis des images d'étiquettes de produits protéinés (whey). "
                        "Extrais TOUTES les valeurs nutritionnelles et l'aminogramme visibles dans l'image. "
                        "Réponds UNIQUEMENT en JSON valide, sans markdown, sans commentaire.\n\n"
                        "Format attendu:\n"
                        '{"nutrition": {"protein_per_100g": 80.0, "carbs_per_100g": 5.2, "sugar_per_100g": 3.1, '
                        '"fat_per_100g": 2.0, "sat_fat_per_100g": 1.2, "kcal_per_100g": 370, "salt_per_100g": 0.5, '
                        '"fiber_per_100g": null}, '
                        '"amino_profile": {"leucine": 10.5, "isoleucine": 5.2, "valine": 5.0, "glutamine": 15.0, '
                        '"arginine": 2.5, "lysine": 8.5}, '
                        '"amino_base": "per_100g_protein", '
                        '"serving_size_g": 30, '
                        '"ingredients_text": "Isolat de protéine de lactosérum, arôme, ...", '
                        '"confidence_notes": "Image claire, tableau lisible"}\n\n'
                        "Règles:\n"
                        "- amino_base: 'per_100g_protein', 'per_100g', ou 'per_serving'\n"
                        "- Valeurs en grammes (convertir mg → g en divisant par 1000)\n"
                        "- Ne pas inventer de valeurs. Mettre null si non visible.\n"
                        "- Si l'image ne contient pas d'information nutritionnelle, retourner {}"
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Extrais toutes les données nutritionnelles et l'aminogramme de cette étiquette produit."
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": hd_url, "detail": "high"},
                        },
                    ],
                },
            ],
            max_tokens=1500,
            temperature=0.1,
        )

        raw = response.choices[0].message.content.strip()
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        import json
        data = json.loads(raw)

        if not data:
            logger.info("[OCR] No nutrition data found in image")
            return evidences

        nutrition = data.get("nutrition", {})
        for field in NUTRITION_FIELDS:
            val = nutrition.get(field)
            if val is not None and isinstance(val, (int, float)):
                evidences.append(NutritionEvidence(
                    field=field, value=float(val),
                    unit="kcal" if field == "kcal_per_100g" else "g",
                    source="ocr_vision",
                    confidence=0.65,
                    raw_snippet=f"OCR: {field}={val}",
                ))

        amino = data.get("amino_profile", {})
        amino_base = data.get("amino_base", "unknown")
        for amino_name, val in amino.items():
            if val is not None and isinstance(val, (int, float)):
                canonical = AMINO_ALIASES.get(amino_name.lower(), amino_name.lower())
                if canonical in AMINO_FIELDS:
                    evidences.append(NutritionEvidence(
                        field=canonical, value=float(val),
                        unit="g", source="ocr_vision",
                        confidence=0.6,
                        raw_snippet=f"OCR: {amino_name}={val}",
                        amino_base=amino_base,
                    ))

        ingredients = data.get("ingredients_text")
        if ingredients and isinstance(ingredients, str) and len(ingredients) > 10:
            evidences.append(NutritionEvidence(
                field="ingredients_text", value=0,
                unit="text", source="ocr_vision",
                confidence=0.55,
                raw_snippet=ingredients[:200],
            ))

        logger.info(f"[OCR] Extracted {len(evidences)} values from image")

    except Exception as e:
        logger.warning(f"[OCR] Error processing image {image_url}: {e}")

    return evidences


# ──────────────────────────────────────────────
# FUSION ENGINE
# ──────────────────────────────────────────────

def should_trigger_ocr(evidences: list[NutritionEvidence]) -> bool:
    fields_found = {e.field for e in evidences if e.confidence >= 0.5}

    has_protein = "protein_per_100g" in fields_found
    has_kcal = "kcal_per_100g" in fields_found
    has_any_amino = bool(fields_found & set(AMINO_FIELDS))

    if not has_protein:
        return True
    if not has_kcal:
        return True
    if not has_any_amino:
        return True

    protein_vals = [e.value for e in evidences if e.field == "protein_per_100g" and e.confidence >= 0.5]
    if protein_vals and (max(protein_vals) >= 96 or min(protein_vals) < 40):
        return True

    return False


def fuse_evidences(all_evidences: list[NutritionEvidence]) -> dict:
    best = {}
    all_raw = {}

    for ev in all_evidences:
        if ev.field == "ingredients_text":
            if ev.field not in best or ev.confidence > best[ev.field].confidence:
                best[ev.field] = ev
            continue

        if ev.field not in best or ev.confidence > best[ev.field].confidence:
            best[ev.field] = ev

        if ev.field not in all_raw:
            all_raw[ev.field] = []
        all_raw[ev.field].append(ev)

    result = {
        "nutrition": {},
        "amino_profile": {},
        "amino_base": "unknown",
        "ingredients_text": None,
        "raw_evidence": [],
        "sources_used": set(),
        "field_count": 0,
        "amino_count": 0,
    }

    for field, ev in best.items():
        result["sources_used"].add(ev.source)
        result["raw_evidence"].append(ev.to_dict())

        if field == "ingredients_text":
            result["ingredients_text"] = ev.raw_snippet
        elif field in NUTRITION_FIELDS:
            result["nutrition"][field] = ev.value
            result["field_count"] += 1
        elif field in AMINO_FIELDS:
            result["amino_profile"][field] = ev.value
            result["amino_count"] += 1
            if ev.amino_base and ev.amino_base != "unknown":
                result["amino_base"] = ev.amino_base

    cross_check = _cross_check_macros(result["nutrition"])
    result["macro_coherent"] = cross_check["coherent"]
    result["coherence_notes"] = cross_check["notes"]

    result["sources_used"] = list(result["sources_used"])

    return result


def _cross_check_macros(nutrition: dict) -> dict:
    notes = []
    coherent = True

    protein = nutrition.get("protein_per_100g")
    carbs = nutrition.get("carbs_per_100g")
    fat = nutrition.get("fat_per_100g")
    kcal = nutrition.get("kcal_per_100g")

    if protein is not None and carbs is not None and fat is not None and kcal is not None:
        expected_kcal = protein * 4 + carbs * 4 + fat * 9
        diff = abs(expected_kcal - kcal)
        if diff > 50:
            coherent = False
            notes.append(f"kcal mismatch: expected ~{expected_kcal:.0f} vs {kcal:.0f} (diff={diff:.0f})")
        else:
            notes.append(f"kcal coherent: expected ~{expected_kcal:.0f} vs {kcal:.0f}")

    if protein is not None:
        if protein >= 96:
            coherent = False
            notes.append(f"protein suspect: {protein}g/100g >= 96")
        elif protein < 40:
            coherent = False
            notes.append(f"protein too low for whey: {protein}g/100g < 40")

    sugar = nutrition.get("sugar_per_100g")
    if sugar is not None and carbs is not None:
        if sugar > carbs + 0.5:
            coherent = False
            notes.append(f"sugar > carbs: {sugar}g vs {carbs}g")

    sat = nutrition.get("sat_fat_per_100g")
    if sat is not None and fat is not None:
        if sat > fat + 0.5:
            coherent = False
            notes.append(f"sat_fat > fat: {sat}g vs {fat}g")

    return {"coherent": coherent, "notes": notes}


# ──────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────

def extract_all_nutrition(
    soup: BeautifulSoup,
    jsonld: dict | None,
    og: dict,
    page_url: str,
    enable_ocr: bool = True,
    force_ocr: bool = False,
    extra_images: list[str] | None = None,
) -> dict:
    all_evidences = []

    source_a = extract_source_a(jsonld, og, soup)
    all_evidences.extend(source_a)
    logger.info(f"[MULTI] Source A (structured): {len(source_a)} values")

    source_b = extract_source_b(soup)
    all_evidences.extend(source_b)
    logger.info(f"[MULTI] Source B (HTML): {len(source_b)} values")

    if enable_ocr and (force_ocr or should_trigger_ocr(all_evidences)):
        nutrition_images = find_nutrition_images(soup, page_url)
        if extra_images:
            nutrition_kw = ["nutri", "valeur", "composition", "amino", "etiquette",
                            "label", "ingredient", "tableau", "information",
                            "nutrition-facts", "back", "dos", "verso", "detail"]
            scored_extra = []
            for img in extra_images:
                if img in nutrition_images:
                    continue
                img_lower = img.lower()
                score = sum(1 for kw in nutrition_kw if kw in img_lower)
                scored_extra.append((score, img))
            scored_extra.sort(key=lambda x: -x[0])
            added = 0
            for score, img in scored_extra:
                if img not in nutrition_images:
                    nutrition_images.append(img)
                    added += 1
                if added >= 5:
                    break
            if added:
                logger.info(f"[MULTI] Added {added} browser-discovered images, total candidates: {len(nutrition_images)}")
        if nutrition_images:
            logger.info(f"[MULTI] Triggering OCR on {len(nutrition_images)} image(s)")
            for img_url in nutrition_images[:2]:
                try:
                    source_c = ocr_nutrition_image(img_url)
                    all_evidences.extend(source_c)
                    logger.info(f"[MULTI] Source C (OCR): {len(source_c)} values from {img_url}")
                except Exception as e:
                    logger.warning(f"[MULTI] OCR failed for {img_url}: {e}")
        else:
            logger.info("[MULTI] No nutrition images found for OCR")
    else:
        logger.info("[MULTI] OCR not triggered (data sufficient)")

    result = fuse_evidences(all_evidences)

    result["total_evidences"] = len(all_evidences)

    return result
