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
    if bcaa_per_100g_prot is not None:
        if bcaa_per_100g_prot > 24:
            score_bcaa = 3
        elif bcaa_per_100g_prot >= 20:
            score_bcaa = 2
        else:
            score_bcaa = 1

    score_leucine = 0
    if leucine_g is not None:
        if leucine_g > 10:
            score_leucine = 2
        elif leucine_g >= 8:
            score_leucine = 1
        else:
            score_leucine = 0

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
    }


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

    if ingredient_count is not None and ingredient_count > 6:
        score -= 1
        details.append(f"liste longue ({ingredient_count} ingredients, -1)")

    score = round(clamp(score, 0, 10), 1)

    return {
        "score_sante": score,
        "details_sante": details,
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
