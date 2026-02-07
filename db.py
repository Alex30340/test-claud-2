import os
import re
import psycopg2
import psycopg2.extras
from datetime import datetime


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            display_name VARCHAR(100) NOT NULL DEFAULT '',
            plan VARCHAR(20) NOT NULL DEFAULT 'free',
            scans_this_month INTEGER NOT NULL DEFAULT 0,
            month_reset DATE NOT NULL DEFAULT CURRENT_DATE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS scans (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            product_count INTEGER NOT NULL DEFAULT 0,
            status VARCHAR(20) NOT NULL DEFAULT 'completed'
        );

        CREATE TABLE IF NOT EXISTS scan_items (
            id SERIAL PRIMARY KEY,
            scan_id INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
            nom VARCHAR(500),
            marque VARCHAR(200),
            url TEXT,
            prix FLOAT,
            devise VARCHAR(10),
            disponibilite VARCHAR(100),
            poids_kg FLOAT,
            prix_par_kg FLOAT,
            proteines_100g FLOAT,
            type_whey VARCHAR(50),
            made_in_france BOOLEAN DEFAULT FALSE,
            has_sucralose BOOLEAN DEFAULT FALSE,
            has_acesulfame_k BOOLEAN DEFAULT FALSE,
            has_aspartame BOOLEAN DEFAULT FALSE,
            has_aminogram BOOLEAN DEFAULT FALSE,
            mentions_bcaa BOOLEAN DEFAULT FALSE,
            score_prix FLOAT,
            score_nutrition FLOAT,
            score_sante FLOAT,
            score_global FLOAT,
            date_recuperation TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(500) NOT NULL,
            brand VARCHAR(200),
            normalized_key VARCHAR(500) UNIQUE,
            type_whey VARCHAR(50) DEFAULT 'unknown',
            proteines_100g FLOAT,
            bcaa_per_100g_prot FLOAT,
            leucine_g FLOAT,
            isoleucine_g FLOAT,
            valine_g FLOAT,
            has_aminogram BOOLEAN DEFAULT FALSE,
            mentions_bcaa BOOLEAN DEFAULT FALSE,
            ingredients TEXT,
            ingredient_count INTEGER,
            has_sucralose BOOLEAN DEFAULT FALSE,
            has_acesulfame_k BOOLEAN DEFAULT FALSE,
            has_aspartame BOOLEAN DEFAULT FALSE,
            has_artificial_flavors BOOLEAN DEFAULT FALSE,
            has_thickeners BOOLEAN DEFAULT FALSE,
            has_colorants BOOLEAN DEFAULT FALSE,
            origin_label VARCHAR(50) DEFAULT 'Inconnu',
            origin_confidence FLOAT DEFAULT 0.3,
            made_in_france BOOLEAN DEFAULT FALSE,
            profil_suspect BOOLEAN DEFAULT FALSE,
            score_proteique FLOAT,
            score_sante FLOAT,
            score_global FLOAT,
            score_final FLOAT,
            needs_review BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS offers (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            merchant VARCHAR(200),
            url TEXT NOT NULL,
            prix FLOAT,
            devise VARCHAR(10) DEFAULT 'EUR',
            poids_kg FLOAT,
            prix_par_kg FLOAT,
            disponibilite VARCHAR(100),
            confidence FLOAT DEFAULT 0.5,
            is_active BOOLEAN DEFAULT TRUE,
            fail_count INTEGER DEFAULT 0,
            last_seen TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id SERIAL PRIMARY KEY,
            run_type VARCHAR(20) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            started_at TIMESTAMP NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMP,
            products_found INTEGER DEFAULT 0,
            offers_updated INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            details TEXT
        );
    """)
    conn.commit()

    new_columns = [
        ("type_whey", "VARCHAR(50)"),
        ("made_in_france", "BOOLEAN DEFAULT FALSE"),
        ("has_sucralose", "BOOLEAN DEFAULT FALSE"),
        ("has_acesulfame_k", "BOOLEAN DEFAULT FALSE"),
        ("has_aspartame", "BOOLEAN DEFAULT FALSE"),
        ("has_aminogram", "BOOLEAN DEFAULT FALSE"),
        ("mentions_bcaa", "BOOLEAN DEFAULT FALSE"),
        ("score_sante", "FLOAT"),
        ("origin_label", "VARCHAR(50) DEFAULT 'Inconnu'"),
        ("origin_confidence", "FLOAT DEFAULT 0.3"),
        ("ingredients", "TEXT"),
        ("has_artificial_flavors", "BOOLEAN DEFAULT FALSE"),
        ("has_thickeners", "BOOLEAN DEFAULT FALSE"),
        ("has_colorants", "BOOLEAN DEFAULT FALSE"),
        ("ingredient_count", "INTEGER"),
        ("bcaa_per_100g_prot", "FLOAT"),
        ("leucine_g", "FLOAT"),
        ("isoleucine_g", "FLOAT"),
        ("valine_g", "FLOAT"),
        ("profil_suspect", "BOOLEAN DEFAULT FALSE"),
        ("score_proteique", "FLOAT"),
        ("score_final", "FLOAT"),
    ]
    for col_name, col_type in new_columns:
        try:
            cur.execute(f"ALTER TABLE scan_items ADD COLUMN {col_name} {col_type}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

    product_new_columns = [
        ("score_final", "FLOAT"),
    ]
    for col_name, col_type in product_new_columns:
        try:
            cur.execute(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

    offer_columns = [
        ("discovery_source", "VARCHAR(200)"),
        ("needs_js_render", "BOOLEAN DEFAULT FALSE"),
        ("price_source", "VARCHAR(100)"),
    ]
    for col_name, col_type in offer_columns:
        try:
            cur.execute(f"ALTER TABLE offers ADD COLUMN {col_name} {col_type}")
            conn.commit()
        except psycopg2.errors.DuplicateColumn:
            conn.rollback()

    cur.close()
    conn.close()


def create_user(email: str, password_hash: str, display_name: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "INSERT INTO users (email, password_hash, display_name) VALUES (%s, %s, %s) RETURNING id, email, display_name, plan, scans_this_month, created_at",
            (email.lower().strip(), password_hash, display_name),
        )
        user = dict(cur.fetchone())
        conn.commit()
        return user
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def get_user_by_email(email: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM users WHERE email = %s", (email.lower().strip(),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def check_and_reset_monthly_usage(user_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT scans_this_month, month_reset, plan FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return 0

    today = datetime.now().date()
    month_reset = row["month_reset"]
    if isinstance(month_reset, datetime):
        month_reset = month_reset.date()

    if today.month != month_reset.month or today.year != month_reset.year:
        cur.execute(
            "UPDATE users SET scans_this_month = 0, month_reset = %s WHERE id = %s",
            (today, user_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return 0

    cur.close()
    conn.close()
    return row["scans_this_month"]


def increment_scan_count(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET scans_this_month = scans_this_month + 1 WHERE id = %s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_scan_limit(plan: str) -> int | None:
    limits = {"free": 3, "pro": None}
    return limits.get(plan, 3)


def save_scan(user_id: int, products: list[dict]) -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scans (user_id, product_count) VALUES (%s, %s) RETURNING id",
        (user_id, len(products)),
    )
    scan_id = cur.fetchone()[0]

    for p in products:
        cur.execute(
            """INSERT INTO scan_items
            (scan_id, nom, marque, url, prix, devise, disponibilite, poids_kg,
             prix_par_kg, proteines_100g, type_whey, made_in_france,
             origin_label, origin_confidence,
             has_sucralose, has_acesulfame_k, has_aspartame,
             has_aminogram, mentions_bcaa, ingredients,
             has_artificial_flavors, has_thickeners, has_colorants, ingredient_count,
             bcaa_per_100g_prot, leucine_g, isoleucine_g, valine_g,
             profil_suspect, score_proteique,
             score_prix, score_sante, score_global, date_recuperation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                scan_id,
                p.get("nom"),
                p.get("marque"),
                p.get("url"),
                p.get("prix"),
                p.get("devise"),
                p.get("disponibilite"),
                p.get("poids_kg"),
                p.get("prix_par_kg"),
                p.get("proteines_100g"),
                p.get("type_whey"),
                p.get("made_in_france", False),
                p.get("origin_label", "Inconnu"),
                p.get("origin_confidence", 0.3),
                p.get("has_sucralose", False),
                p.get("has_acesulfame_k", False),
                p.get("has_aspartame", False),
                p.get("has_aminogram", False),
                p.get("mentions_bcaa", False),
                p.get("ingredients"),
                p.get("has_artificial_flavors", False),
                p.get("has_thickeners", False),
                p.get("has_colorants", False),
                p.get("ingredient_count"),
                p.get("bcaa_per_100g_prot"),
                p.get("leucine_g"),
                p.get("isoleucine_g"),
                p.get("valine_g"),
                p.get("profil_suspect", False),
                p.get("score_proteique"),
                p.get("score_prix"),
                p.get("score_sante"),
                p.get("score_global"),
                p.get("date_recuperation"),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    return scan_id


def get_user_scans(user_id: int, limit: int = 20) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT id, created_at, product_count, status FROM scans WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
        (user_id, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def get_scan_items(scan_id: int, user_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """SELECT si.* FROM scan_items si
           JOIN scans s ON si.scan_id = s.id
           WHERE si.scan_id = %s AND s.user_id = %s
           ORDER BY si.score_global DESC NULLS LAST""",
        (scan_id, user_id),
    )
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def can_user_scan(user_id: int, plan: str) -> bool:
    scans_used = check_and_reset_monthly_usage(user_id)
    limit = get_scan_limit(plan)
    if limit is None:
        return True
    return scans_used < limit


# ---------------------------------------------------------------------------
# Product catalog functions
# ---------------------------------------------------------------------------

def normalize_product_key(brand: str, name: str) -> str:
    combined = f"{brand or ''} {name}".lower().strip()
    combined = re.sub(r'[^a-z0-9\s]', '', combined)
    combined = re.sub(r'\s+', ' ', combined).strip()
    combined = re.sub(r'\d+\s*(kg|g|gramme|ml|l)\b', '', combined).strip()
    combined = re.sub(r'\s+', ' ', combined).strip()
    return combined


def upsert_product(product_data: dict) -> int:
    name = product_data.get("name", "")
    brand = product_data.get("brand", "")
    normalized_key = normalize_product_key(brand, name)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id FROM products WHERE normalized_key = %s", (normalized_key,))
        existing = cur.fetchone()

        fields = [
            "name", "brand", "normalized_key", "type_whey", "proteines_100g",
            "bcaa_per_100g_prot", "leucine_g", "isoleucine_g", "valine_g",
            "has_aminogram", "mentions_bcaa", "ingredients", "ingredient_count",
            "has_sucralose", "has_acesulfame_k", "has_aspartame",
            "has_artificial_flavors", "has_thickeners", "has_colorants",
            "origin_label", "origin_confidence", "made_in_france",
            "profil_suspect", "score_proteique", "score_sante", "score_global",
            "score_final", "needs_review",
        ]

        values = {
            "name": name,
            "brand": brand,
            "normalized_key": normalized_key,
            "type_whey": product_data.get("type_whey", "unknown"),
            "proteines_100g": product_data.get("proteines_100g"),
            "bcaa_per_100g_prot": product_data.get("bcaa_per_100g_prot"),
            "leucine_g": product_data.get("leucine_g"),
            "isoleucine_g": product_data.get("isoleucine_g"),
            "valine_g": product_data.get("valine_g"),
            "has_aminogram": product_data.get("has_aminogram", False),
            "mentions_bcaa": product_data.get("mentions_bcaa", False),
            "ingredients": product_data.get("ingredients"),
            "ingredient_count": product_data.get("ingredient_count"),
            "has_sucralose": product_data.get("has_sucralose", False),
            "has_acesulfame_k": product_data.get("has_acesulfame_k", False),
            "has_aspartame": product_data.get("has_aspartame", False),
            "has_artificial_flavors": product_data.get("has_artificial_flavors", False),
            "has_thickeners": product_data.get("has_thickeners", False),
            "has_colorants": product_data.get("has_colorants", False),
            "origin_label": product_data.get("origin_label", "Inconnu"),
            "origin_confidence": product_data.get("origin_confidence", 0.3),
            "made_in_france": product_data.get("made_in_france", False),
            "profil_suspect": product_data.get("profil_suspect", False),
            "score_proteique": product_data.get("score_proteique"),
            "score_sante": product_data.get("score_sante"),
            "score_global": product_data.get("score_global"),
            "score_final": product_data.get("score_final"),
            "needs_review": product_data.get("needs_review", False),
        }

        if existing:
            update_fields = [f for f in fields if f != "normalized_key"]
            set_clause = ", ".join(f"{f} = %s" for f in update_fields)
            set_clause += ", updated_at = NOW()"
            params = [values[f] for f in update_fields] + [existing["id"]]
            cur.execute(f"UPDATE products SET {set_clause} WHERE id = %s RETURNING id", params)
            product_id = cur.fetchone()["id"]
        else:
            cols = ", ".join(fields)
            placeholders = ", ".join(["%s"] * len(fields))
            params = [values[f] for f in fields]
            cur.execute(
                f"INSERT INTO products ({cols}) VALUES ({placeholders}) RETURNING id",
                params,
            )
            product_id = cur.fetchone()["id"]

        conn.commit()
        return product_id
    finally:
        cur.close()
        conn.close()


def upsert_offer(product_id: int, offer_data: dict) -> int:
    url = offer_data.get("url", "")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT id FROM offers WHERE product_id = %s AND url = %s",
            (product_id, url),
        )
        existing = cur.fetchone()

        merchant = offer_data.get("merchant")
        prix = offer_data.get("prix")
        devise = offer_data.get("devise", "EUR")
        poids_kg = offer_data.get("poids_kg")
        prix_par_kg = offer_data.get("prix_par_kg")
        disponibilite = offer_data.get("disponibilite")
        confidence = offer_data.get("confidence", 0.5)
        discovery_source = offer_data.get("discovery_source")
        needs_js_render = offer_data.get("needs_js_render", False)
        price_source = offer_data.get("price_source")

        if existing:
            cur.execute(
                """UPDATE offers
                   SET merchant = %s, prix = %s, devise = %s, poids_kg = %s,
                       prix_par_kg = %s, disponibilite = %s, confidence = %s,
                       discovery_source = COALESCE(%s, discovery_source),
                       needs_js_render = %s, price_source = %s,
                       is_active = TRUE, fail_count = 0, last_seen = NOW(), updated_at = NOW()
                   WHERE id = %s RETURNING id""",
                (merchant, prix, devise, poids_kg, prix_par_kg, disponibilite, confidence,
                 discovery_source, needs_js_render, price_source, existing["id"]),
            )
            offer_id = cur.fetchone()["id"]
        else:
            cur.execute(
                """INSERT INTO offers
                   (product_id, merchant, url, prix, devise, poids_kg, prix_par_kg,
                    disponibilite, confidence, discovery_source, needs_js_render, price_source)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (product_id, merchant, url, prix, devise, poids_kg, prix_par_kg,
                 disponibilite, confidence, discovery_source, needs_js_render, price_source),
            )
            offer_id = cur.fetchone()["id"]

        conn.commit()
        return offer_id
    finally:
        cur.close()
        conn.close()


def get_all_products(min_confidence: float = 0.0, limit: int = 200) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """WITH best_offers AS (
                SELECT DISTINCT ON (product_id)
                    product_id,
                    prix AS offer_prix,
                    prix_par_kg AS offer_prix_par_kg,
                    url AS offer_url,
                    merchant AS offer_merchant,
                    confidence AS offer_confidence,
                    poids_kg AS offer_poids_kg
                FROM offers
                WHERE is_active = TRUE
                ORDER BY product_id, confidence DESC, updated_at DESC
            )
            SELECT p.*, bo.offer_prix, bo.offer_prix_par_kg, bo.offer_url,
                   bo.offer_merchant, bo.offer_confidence, bo.offer_poids_kg
            FROM products p
            LEFT JOIN best_offers bo ON p.id = bo.product_id
            WHERE bo.offer_confidence >= %s OR bo.offer_confidence IS NULL
            ORDER BY p.score_final DESC NULLS LAST, p.score_global DESC NULLS LAST
            LIMIT %s""",
            (min_confidence, limit),
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def get_product_offers(product_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM offers WHERE product_id = %s ORDER BY confidence DESC, updated_at DESC",
            (product_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def get_active_offers(min_confidence: float = 0.3) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT id, product_id, url, confidence, last_seen, fail_count
               FROM offers
               WHERE is_active = TRUE AND confidence >= %s
               ORDER BY last_seen ASC""",
            (min_confidence,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def update_offer_price(offer_id: int, prix: float | None, prix_par_kg: float | None,
                       disponibilite: str = "", confidence: float = 0.5):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE offers
               SET prix = %s, prix_par_kg = %s, disponibilite = %s,
                   confidence = %s, fail_count = 0, last_seen = NOW(), updated_at = NOW()
               WHERE id = %s""",
            (prix, prix_par_kg, disponibilite, confidence, offer_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def mark_offer_failed(offer_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE offers
               SET fail_count = fail_count + 1,
                   is_active = CASE WHEN fail_count + 1 >= 3 THEN FALSE ELSE is_active END,
                   updated_at = NOW()
               WHERE id = %s""",
            (offer_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def create_pipeline_run(run_type: str) -> int:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO pipeline_runs (run_type) VALUES (%s) RETURNING id",
            (run_type,),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        cur.close()
        conn.close()


def update_pipeline_run(run_id: int, status: str, products_found: int = 0,
                         offers_updated: int = 0, errors: int = 0, details: str = ""):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE pipeline_runs
               SET status = %s, finished_at = NOW(), products_found = %s,
                   offers_updated = %s, errors = %s, details = %s
               WHERE id = %s""",
            (status, products_found, offers_updated, errors, details, run_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_pipeline_runs(limit: int = 10) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT %s",
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()


def get_catalog_stats() -> dict:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT COUNT(*) AS total_products FROM products")
        total_products = cur.fetchone()["total_products"]

        cur.execute("SELECT COUNT(*) AS total_active_offers FROM offers WHERE is_active = TRUE")
        total_active_offers = cur.fetchone()["total_active_offers"]

        cur.execute("SELECT AVG(confidence) AS avg_confidence FROM offers WHERE is_active = TRUE")
        row = cur.fetchone()
        avg_confidence = round(row["avg_confidence"], 3) if row["avg_confidence"] is not None else 0.0

        cur.execute(
            "SELECT started_at FROM pipeline_runs WHERE run_type = 'discovery' ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        last_discovery = row["started_at"] if row else None

        cur.execute(
            "SELECT started_at FROM pipeline_runs WHERE run_type = 'refresh' ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        last_refresh = row["started_at"] if row else None

        cur.execute("SELECT COUNT(*) AS cnt FROM products WHERE needs_review = TRUE")
        products_needing_review = cur.fetchone()["cnt"]

        return {
            "total_products": total_products,
            "total_active_offers": total_active_offers,
            "avg_confidence": avg_confidence,
            "last_discovery": last_discovery,
            "last_refresh": last_refresh,
            "products_needing_review": products_needing_review,
        }
    finally:
        cur.close()
        conn.close()
