def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def calculate_price_score(price_per_kg: float | None) -> float | None:
    if price_per_kg is None:
        return None
    if price_per_kg <= 20:
        return 100.0
    if price_per_kg >= 80:
        return 0.0
    return round(100 * (80 - price_per_kg) / (80 - 20), 1)


def calculate_nutrition_score(protein_per_100g: float | None) -> float | None:
    if protein_per_100g is None:
        return None
    if protein_per_100g <= 60:
        return 0.0
    if protein_per_100g >= 90:
        return 100.0
    return round(100 * (protein_per_100g - 60) / (90 - 60), 1)


def calculate_health_score(
    protein_per_100g: float | None,
    whey_type: str | None,
    made_in_france: bool | None,
    has_sucralose: bool | None,
    has_acesulfame_k: bool | None,
    has_aspartame: bool | None,
    has_aminogram: bool | None,
    mentions_bcaa: bool | None,
    origin_label: str | None = None,
    origin_confidence: float | None = None,
) -> float | None:
    score = 50.0

    wt = (whey_type or "unknown").lower()
    if wt == "native":
        score += 18
    elif wt == "isolate":
        score += 14
    elif wt == "hydrolysate":
        score += 12
    elif wt == "concentrate":
        score -= 8

    ol = (origin_label or "").strip()
    oc = origin_confidence if origin_confidence is not None else 0.3

    if ol == "France":
        score += round(8 * oc, 1)
    elif ol == "EU":
        score += round(4 * oc, 1)
    elif made_in_france is True:
        score += 8

    penalty = 0
    if has_sucralose:
        penalty += 10
    if has_acesulfame_k:
        penalty += 8
    if has_aspartame:
        penalty += 18
    score -= min(penalty, 22)

    if has_aminogram:
        score += 8
    elif mentions_bcaa:
        score += 3

    if protein_per_100g is not None:
        if protein_per_100g >= 85:
            score += 6
        elif protein_per_100g <= 70:
            score -= 6

    return round(clamp(score), 1)


def calculate_global_score(
    price_per_kg: float | None,
    protein_per_100g: float | None,
    health_score: float | None = None,
) -> float | None:
    price_score = calculate_price_score(price_per_kg)
    nutrition_score = calculate_nutrition_score(protein_per_100g)

    weights = {"health": 0.55, "nutrition": 0.20, "price": 0.25}

    parts = []
    if health_score is not None:
        parts.append(("health", health_score))
    if nutrition_score is not None:
        parts.append(("nutrition", nutrition_score))
    if price_score is not None:
        parts.append(("price", price_score))

    if not parts:
        return None

    total_w = sum(weights[k] for k, _ in parts)
    val = sum((weights[k] / total_w) * s for k, s in parts)
    return round(clamp(val), 1)
