import os
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
    ]
    for col_name, col_type in new_columns:
        try:
            cur.execute(f"ALTER TABLE scan_items ADD COLUMN {col_name} {col_type}")
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
