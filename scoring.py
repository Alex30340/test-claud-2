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


def calculate_global_score(
    price_per_kg: float | None,
    protein_per_100g: float | None,
) -> float | None:
    price_score = calculate_price_score(price_per_kg)
    nutrition_score = calculate_nutrition_score(protein_per_100g)

    if price_score is not None and nutrition_score is not None:
        return round(0.5 * price_score + 0.5 * nutrition_score, 1)
    if price_score is not None:
        return price_score
    if nutrition_score is not None:
        return nutrition_score
    return None
