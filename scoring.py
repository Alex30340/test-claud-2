def clamp(x: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, x))


def calculate_protein_score(
    protein_per_100g: float | None,
    bcaa_per_100g_prot: float | None = None,
    leucine_g: float | None = None,
    isoleucine_g: float | None = None,
    valine_g: float | None = None,
) -> dict:
    score_prot_pct = 0
    if protein_per_100g is not None:
        if protein_per_100g > 85:
            score_prot_pct = 5
        elif protein_per_100g >= 80:
            score_prot_pct = 4
        elif protein_per_100g >= 75:
            score_prot_pct = 3
        elif protein_per_100g >= 70:
            score_prot_pct = 2
        else:
            score_prot_pct = 1

    score_bcaa = 0
    bcaa_missing = False
    if bcaa_per_100g_prot is not None:
        if bcaa_per_100g_prot > 24:
            score_bcaa = 3
        elif bcaa_per_100g_prot >= 20:
            score_bcaa = 2
        else:
            score_bcaa = 1
    else:
        bcaa_missing = True
        score_bcaa = 1.5

    score_leucine = 0
    leucine_missing = False
    if leucine_g is not None:
        if leucine_g > 10:
            score_leucine = 2
        elif leucine_g >= 8:
            score_leucine = 1
        else:
            score_leucine = 0
    else:
        leucine_missing = True
        score_leucine = 0.8

    malus_profil = 0
    profil_suspect = False
    if leucine_g is not None and isoleucine_g is not None and valine_g is not None:
        if leucine_g > 0 and isoleucine_g > 0 and valine_g > 0:
            ratio_iso = leucine_g / isoleucine_g if isoleucine_g > 0 else 99
            ratio_val = leucine_g / valine_g if valine_g > 0 else 99
            expected_ratio = 2.0
            if abs(ratio_iso - expected_ratio) > 1.0 or abs(ratio_val - expected_ratio) > 1.0:
                profil_suspect = True
                malus_profil = -2
            elif abs(ratio_iso - expected_ratio) > 0.5 or abs(ratio_val - expected_ratio) > 0.5:
                profil_suspect = True
                malus_profil = -1

    total = score_prot_pct + score_bcaa + score_leucine + malus_profil
    total = round(clamp(total, 0, 10), 1)

    return {
        "score_proteique": total,
        "score_prot_pct": score_prot_pct,
        "score_bcaa": score_bcaa,
        "score_leucine": score_leucine,
        "malus_profil": malus_profil,
        "profil_suspect": profil_suspect,
        "bcaa_missing": bcaa_missing,
        "leucine_missing": leucine_missing,
    }


def ingredient_count_penalty(ingredient_count: int | None) -> float:
    if ingredient_count is None:
        return 0
    if ingredient_count > 20:
        return -3.0
    if ingredient_count >= 15:
        return -2.0
    if ingredient_count >= 10:
        return -1.0
    if ingredient_count >= 7:
        return -0.5
    return 0


def calculate_health_score(
    has_sucralose: bool = False,
    has_acesulfame_k: bool = False,
    has_aspartame: bool = False,
    has_artificial_flavors: bool = False,
    has_thickeners: bool = False,
    has_colorants: bool = False,
    ingredient_count: int | None = None,
) -> dict:
    score = 10.0
    details = []

    sweetener_count = sum([
        bool(has_sucralose),
        bool(has_acesulfame_k),
        bool(has_aspartame),
    ])

    if sweetener_count >= 2:
        score -= 3
        details.append("plusieurs edulcorants (-3)")
    elif sweetener_count == 1:
        score -= 2
        details.append("edulcorant artificiel (-2)")

    if has_artificial_flavors:
        score -= 1
        details.append("aromes artificiels (-1)")

    if has_thickeners:
        score -= 1
        details.append("epaississants (-1)")

    if has_colorants:
        score -= 1
        details.append("colorants (-1)")

    ing_penalty = ingredient_count_penalty(ingredient_count)
    if ing_penalty < 0:
        score += ing_penalty
        details.append(f"liste longue ({ingredient_count} ingredients, {ing_penalty})")

    score = round(clamp(score, 0, 10), 1)

    return {
        "score_sante": score,
        "details_sante": details,
        "ingredient_penalty": ing_penalty,
    }


def calculate_global_score(
    score_proteique: float | None,
    score_sante: float | None,
) -> float | None:
    if score_proteique is None and score_sante is None:
        return None

    if score_proteique is not None and score_sante is not None:
        return round(score_proteique * 0.6 + score_sante * 0.4, 1)

    if score_proteique is not None:
        return round(score_proteique, 1)

    return round(score_sante, 1)


def calculate_price_score(price_per_kg: float | None) -> float | None:
    if price_per_kg is None:
        return None
    if price_per_kg <= 20:
        return 100.0
    if price_per_kg >= 80:
        return 0.0
    return round(100 * (80 - price_per_kg) / (80 - 20), 1)


def calculate_price_score_10(price_per_kg: float | None) -> float | None:
    if price_per_kg is None:
        return None
    if price_per_kg <= 15:
        return 10.0
    if price_per_kg <= 25:
        return 9.0
    if price_per_kg <= 35:
        return 8.0
    if price_per_kg <= 45:
        return 7.0
    if price_per_kg <= 55:
        return 6.0
    if price_per_kg <= 65:
        return 5.0
    if price_per_kg <= 80:
        return 4.0
    if price_per_kg <= 100:
        return 3.0
    if price_per_kg <= 130:
        return 2.0
    if price_per_kg <= 160:
        return 1.0
    return 0.0


def premium_bonus(
    protein_per_100g: float | None = None,
    leucine_g: float | None = None,
    has_aminogram: bool = False,
    origin_label: str = "Inconnu",
) -> dict:
    bonus = 0.0
    reasons = []

    if protein_per_100g is not None and protein_per_100g >= 90:
        bonus += 0.5
        reasons.append(f"ultra-pure ({protein_per_100g:.0f}g/100g)")

    if leucine_g is not None and leucine_g >= 10.5:
        bonus += 0.3
        reasons.append(f"leucine elevee ({leucine_g:.1f}g)")

    if has_aminogram:
        bonus += 0.3
        reasons.append("aminogramme present")

    if origin_label == "France":
        bonus += 0.2
        reasons.append("fabrication France")

    return {
        "bonus": round(bonus, 1),
        "reasons": reasons,
    }


def transparency_penalty(
    bcaa_missing: bool = False,
    leucine_missing: bool = False,
) -> dict:
    penalty = 0.0
    reasons = []

    if bcaa_missing:
        penalty -= 0.15
        reasons.append("BCAA non communiques")
    if leucine_missing:
        penalty -= 0.15
        reasons.append("leucine non communiquee")

    return {
        "penalty": round(penalty, 2),
        "reasons": reasons,
        "is_low_transparency": len(reasons) > 0,
    }


def calculate_final_score_10(
    score_proteique: float | None,
    score_sante: float | None,
    price_per_kg: float | None = None,
    protein_per_100g: float | None = None,
    leucine_g: float | None = None,
    has_aminogram: bool = False,
    origin_label: str = "Inconnu",
    bcaa_missing: bool = False,
    leucine_missing: bool = False,
    ingredient_count: int | None = None,
) -> dict:
    if score_proteique is None and score_sante is None:
        return {
            "score_final": None,
            "price_score_10": None,
            "premium_bonus": 0,
            "premium_reasons": [],
            "transparency_penalty": 0,
            "transparency_reasons": [],
            "is_low_transparency": False,
            "is_top_qualite": False,
        }

    sp = score_proteique if score_proteique is not None else 5.0
    ss = score_sante if score_sante is not None else 5.0

    ps10 = calculate_price_score_10(price_per_kg)
    price_component = ps10 if ps10 is not None else 5.0

    base = (sp * 0.50) + (ss * 0.35) + (price_component * 0.15)

    prem = premium_bonus(
        protein_per_100g=protein_per_100g,
        leucine_g=leucine_g,
        has_aminogram=has_aminogram,
        origin_label=origin_label,
    )

    transp = transparency_penalty(
        bcaa_missing=bcaa_missing,
        leucine_missing=leucine_missing,
    )

    final = base + prem["bonus"] + transp["penalty"]
    final = round(clamp(final, 0, 10), 1)

    is_top = (
        score_proteique is not None and score_proteique >= 8.5 and
        score_sante is not None and score_sante >= 8.5 and
        (ingredient_count is None or ingredient_count <= 9)
    )

    return {
        "score_final": final,
        "price_score_10": ps10,
        "premium_bonus": prem["bonus"],
        "premium_reasons": prem["reasons"],
        "transparency_penalty": transp["penalty"],
        "transparency_reasons": transp["reasons"],
        "is_low_transparency": transp["is_low_transparency"],
        "is_top_qualite": is_top,
    }
