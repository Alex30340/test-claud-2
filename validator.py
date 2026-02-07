import re
import logging

logger = logging.getLogger(__name__)

WHEY_PRICE_MIN = 8.0
WHEY_PRICE_MAX = 200.0
WEIGHT_MIN_KG = 0.2
WEIGHT_MAX_KG = 5.0
PRICE_PER_KG_MAX = 200.0


def validate_price(price: float | None) -> float | None:
    if price is None:
        return None
    if WHEY_PRICE_MIN <= price <= WHEY_PRICE_MAX:
        return round(price, 2)
    return None


def validate_weight(weight: float | None) -> float | None:
    if weight is None:
        return None
    if WEIGHT_MIN_KG <= weight <= WEIGHT_MAX_KG:
        return round(weight, 3)
    return None


def validate_price_per_kg(price: float | None, weight: float | None) -> float | None:
    if price is None or weight is None or weight <= 0:
        return None
    ppk = round(price / weight, 2)
    if ppk > PRICE_PER_KG_MAX:
        logger.info(f"Price per kg suspicious: {ppk} EUR/kg (price={price}, weight={weight})")
        return None
    return ppk


def compute_confidence_v2(data: dict, has_jsonld: bool, needs_js_render: bool = False) -> float:
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

    if needs_js_render and data.get("prix") is None:
        confidence = min(confidence, 0.3)

    return min(1.0, max(0.0, confidence))
