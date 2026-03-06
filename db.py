import os
import re
import time
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
from datetime import datetime

logger = logging.getLogger(__name__)

_connection_pool = None

_DB_RETRY_ATTEMPTS = 5
_DB_RETRY_BASE_DELAY = 2


def _get_pool():
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        dsn = os.environ["DATABASE_URL"]
        for attempt in range(1, _DB_RETRY_ATTEMPTS + 1):
            try:
                _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2, maxconn=10, dsn=dsn,
                )
                return _connection_pool
            except psycopg2.OperationalError as e:
                if attempt < _DB_RETRY_ATTEMPTS:
                    delay = _DB_RETRY_BASE_DELAY * attempt
                    logger.warning(f"[DB] Pool creation attempt {attempt}/{_DB_RETRY_ATTEMPTS} failed ({e}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise
    return _connection_pool


def _check_conn(conn):
    try:
        if conn.closed:
            return False
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return True
    except Exception:
        return False


def _connect_with_retry():
    dsn = os.environ["DATABASE_URL"]
    for attempt in range(1, _DB_RETRY_ATTEMPTS + 1):
        try:
            conn = psycopg2.connect(dsn)
            return conn
        except psycopg2.OperationalError as e:
            err_msg = str(e).lower()
            is_transient = (
                "endpoint is disabled" in err_msg
                or "désactivé" in err_msg
                or "the endpoint" in err_msg
                or "connection refused" in err_msg
                or "timeout" in err_msg
                or "could not connect" in err_msg
                or "server closed" in err_msg
                or "connection reset" in err_msg
            )
            if is_transient and attempt < _DB_RETRY_ATTEMPTS:
                delay = _DB_RETRY_BASE_DELAY * attempt
                logger.warning(f"[DB] Connection attempt {attempt}/{_DB_RETRY_ATTEMPTS} failed ({e}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise


def get_connection():
    try:
        pool = _get_pool()
        conn = pool.getconn()
        if not _check_conn(conn):
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = _connect_with_retry()
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError:
        global _connection_pool
        _connection_pool = None
        return _connect_with_retry()
    except Exception:
        return _connect_with_retry()


def release_connection(conn):
    try:
        if conn.closed:
            return
        conn.rollback()
        pool = _get_pool()
        pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


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

        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
            title VARCHAR(255),
            comment TEXT,
            purchased_from VARCHAR(255),
            is_flagged BOOLEAN DEFAULT FALSE,
            is_hidden BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS recommendations (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            usage_context VARCHAR(50) NOT NULL,
            level VARCHAR(50),
            pros TEXT,
            cons TEXT,
            comment TEXT NOT NULL,
            is_hidden BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            prix FLOAT,
            prix_par_kg FLOAT,
            merchant VARCHAR(255),
            recorded_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS price_alerts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            target_price FLOAT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            message TEXT NOT NULL,
            link_page VARCHAR(50),
            link_product_id INTEGER,
            is_read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS user_preferences (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
            weight_protein FLOAT DEFAULT 50,
            weight_health FLOAT DEFAULT 35,
            weight_price FLOAT DEFAULT 15,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_offers_product_id ON offers(product_id);
        CREATE INDEX IF NOT EXISTS idx_offers_active ON offers(product_id, confidence DESC, updated_at DESC) WHERE is_active = TRUE;
        CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id);
        CREATE INDEX IF NOT EXISTS idx_scans_user_id ON scans(user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_scan_items_scan_id ON scan_items(scan_id);
        CREATE INDEX IF NOT EXISTS idx_products_score ON products(score_final DESC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_recommendations_product_id ON recommendations(product_id);
        CREATE INDEX IF NOT EXISTS idx_price_history_product ON price_history(product_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_price_alerts_user ON price_alerts(user_id, is_active) WHERE is_active = TRUE;
        CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_read, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_user_preferences_user ON user_preferences(user_id);
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
        ("protein_source", "VARCHAR(50)"),
        ("protein_confidence", "FLOAT DEFAULT 0.0"),
        ("protein_suspect", "BOOLEAN DEFAULT FALSE"),
        ("image_url", "TEXT"),
        ("carbs_per_100g", "FLOAT"),
        ("sugar_per_100g", "FLOAT"),
        ("fat_per_100g", "FLOAT"),
        ("sat_fat_per_100g", "FLOAT"),
        ("kcal_per_100g", "FLOAT"),
        ("salt_per_100g", "FLOAT"),
        ("fiber_per_100g", "FLOAT"),
        ("amino_profile", "JSONB"),
        ("amino_base", "VARCHAR(30) DEFAULT 'unknown'"),
        ("raw_evidence", "JSONB"),
        ("nutrition_sources", "TEXT"),
        ("macro_coherent", "BOOLEAN DEFAULT TRUE"),
        ("glutamine_g", "FLOAT"),
        ("arginine_g", "FLOAT"),
        ("lysine_g", "FLOAT"),
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
    release_connection(conn)

    _seed_if_empty()


def _update_missing_images(conn, cur, seed_products):
    try:
        cur.execute("SELECT id, name FROM products WHERE image_url IS NULL OR image_url = ''")
        missing = cur.fetchall()
        if not missing:
            return
        seed_by_name = {}
        for sp in seed_products:
            if sp.get("image_url"):
                seed_by_name[sp.get("name", "")] = sp["image_url"]
        updated = 0
        for pid, pname in missing:
            img = seed_by_name.get(pname)
            if img:
                cur.execute("UPDATE products SET image_url = %s WHERE id = %s", (img, pid))
                updated += 1
        if updated > 0:
            conn.commit()
            logger.info(f"[SEED] Updated {updated} product images")
    except Exception as e:
        conn.rollback()
        logger.warning(f"[SEED] Image update error: {e}")


def _seed_if_empty():
    import json, os
    seed_path = os.path.join(os.path.dirname(__file__), "seed_data.json")
    if not os.path.exists(seed_path):
        return

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM products")
        count = cur.fetchone()[0]

        with open(seed_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        offers = data.get("offers", [])

        if count >= 10:
            _update_missing_images(conn, cur, products)
            cur.close()
            release_connection(conn)
            return

        if not products:
            cur.close()
            release_connection(conn)
            return

        logger.info(f"[SEED] Seeding {len(products)} products and {len(offers)} offers...")

        prod_cols = None
        for p in products:
            if prod_cols is None:
                prod_cols = [k for k in p.keys() if k != "id"]
            vals = []
            for c in prod_cols:
                v = p.get(c)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                vals.append(v)
            placeholders = ", ".join(["%s"] * len(prod_cols))
            col_names = ", ".join(prod_cols)
            try:
                cur.execute(
                    f"INSERT INTO products ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    vals
                )
            except Exception as e:
                conn.rollback()
                logger.warning(f"[SEED] Error inserting product {p.get('name', '?')}: {e}")
                continue
        conn.commit()

        cur.execute("SELECT id, name FROM products")
        prod_map = {}
        for row in cur.fetchall():
            prod_map[row[1]] = row[0]

        old_to_new = {}
        for p in products:
            old_id = p.get("id")
            name = p.get("name")
            if name in prod_map:
                old_to_new[old_id] = prod_map[name]

        offer_cols_skip = {"id"}
        for o in offers:
            old_pid = o.get("product_id")
            new_pid = old_to_new.get(old_pid)
            if not new_pid:
                continue
            o_copy = {k: v for k, v in o.items() if k not in offer_cols_skip}
            o_copy["product_id"] = new_pid
            cols = list(o_copy.keys())
            vals = []
            for c in cols:
                v = o_copy[c]
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                vals.append(v)
            placeholders = ", ".join(["%s"] * len(cols))
            col_names = ", ".join(cols)
            try:
                cur.execute(
                    f"INSERT INTO offers ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    vals
                )
            except Exception as e:
                conn.rollback()
                logger.warning(f"[SEED] Error inserting offer: {e}")
                continue
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM products")
        new_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM offers")
        offer_count = cur.fetchone()[0]
        logger.info(f"[SEED] Seeding complete: {new_count} products, {offer_count} offers")

    except Exception as e:
        conn.rollback()
        logger.error(f"[SEED] Seeding error: {e}")
    finally:
        cur.close()
        release_connection(conn)


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
        release_connection(conn)


def get_user_by_email(email: str) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE email = %s", (email.lower().strip(),))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        release_connection(conn)


def check_and_reset_monthly_usage(user_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT scans_this_month, month_reset, plan FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        release_connection(conn)
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
        release_connection(conn)
        return 0

    cur.close()
    release_connection(conn)
    return row["scans_this_month"]


def increment_scan_count(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET scans_this_month = scans_this_month + 1 WHERE id = %s", (user_id,))
        conn.commit()
    finally:
        cur.close()
        release_connection(conn)


def get_scan_limit(plan: str) -> int | None:
    limits = {"free": 3, "pro": None}
    return limits.get(plan, 3)


def save_scan(user_id: int, products: list[dict]) -> int:
    conn = get_connection()
    cur = conn.cursor()
    try:
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
        return scan_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        release_connection(conn)


def get_user_scans(user_id: int, limit: int = 20) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT id, created_at, product_count, status FROM scans WHERE user_id = %s ORDER BY created_at DESC LIMIT %s",
            (user_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_scan_items(scan_id: int, user_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT si.* FROM scan_items si
               JOIN scans s ON si.scan_id = s.id
               WHERE si.scan_id = %s AND s.user_id = %s
               ORDER BY si.score_global DESC NULLS LAST""",
            (scan_id, user_id),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


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
            "profil_suspect", "protein_source", "protein_confidence", "protein_suspect",
            "score_proteique", "score_sante", "score_global",
            "score_final", "needs_review", "image_url",
            "carbs_per_100g", "sugar_per_100g", "fat_per_100g", "sat_fat_per_100g",
            "kcal_per_100g", "salt_per_100g", "fiber_per_100g",
            "amino_profile", "amino_base", "raw_evidence", "nutrition_sources",
            "macro_coherent", "glutamine_g", "arginine_g", "lysine_g",
        ]

        import json as _json
        amino_profile = product_data.get("amino_profile")
        raw_evidence = product_data.get("raw_evidence")

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
            "protein_source": product_data.get("protein_source"),
            "protein_confidence": product_data.get("protein_confidence", 0.0),
            "protein_suspect": product_data.get("protein_suspect", False),
            "score_proteique": product_data.get("score_proteique"),
            "score_sante": product_data.get("score_sante"),
            "score_global": product_data.get("score_global"),
            "score_final": product_data.get("score_final"),
            "needs_review": product_data.get("needs_review", False),
            "image_url": product_data.get("image_url"),
            "carbs_per_100g": product_data.get("carbs_per_100g"),
            "sugar_per_100g": product_data.get("sugar_per_100g"),
            "fat_per_100g": product_data.get("fat_per_100g"),
            "sat_fat_per_100g": product_data.get("sat_fat_per_100g"),
            "kcal_per_100g": product_data.get("kcal_per_100g"),
            "salt_per_100g": product_data.get("salt_per_100g"),
            "fiber_per_100g": product_data.get("fiber_per_100g"),
            "amino_profile": _json.dumps(amino_profile) if amino_profile else None,
            "amino_base": product_data.get("amino_base", "unknown"),
            "raw_evidence": _json.dumps(raw_evidence) if raw_evidence else None,
            "nutrition_sources": product_data.get("nutrition_sources"),
            "macro_coherent": product_data.get("macro_coherent", True),
            "glutamine_g": product_data.get("glutamine_g"),
            "arginine_g": product_data.get("arginine_g"),
            "lysine_g": product_data.get("lysine_g"),
        }

        if existing:
            preserve_fields = {
                "proteines_100g", "ingredients", "ingredient_count", "image_url",
                "score_proteique", "score_sante", "score_global", "score_final",
                "bcaa_per_100g_prot", "leucine_g", "isoleucine_g", "valine_g",
                "amino_profile", "amino_base", "raw_evidence", "nutrition_sources",
                "carbs_per_100g", "sugar_per_100g", "fat_per_100g", "sat_fat_per_100g",
                "kcal_per_100g", "salt_per_100g", "fiber_per_100g",
                "glutamine_g", "arginine_g", "lysine_g",
                "protein_source", "protein_confidence",
                "origin_label", "origin_confidence",
                "type_whey",
            }
            cur.execute("SELECT * FROM products WHERE id = %s", (existing["id"],))
            existing_row = cur.fetchone()

            update_fields = []
            for f in fields:
                if f == "normalized_key":
                    continue
                new_val = values[f]
                if f in preserve_fields:
                    old_val = existing_row.get(f)
                    if old_val is not None and old_val != "" and old_val != "unknown" and old_val != "Inconnu":
                        if new_val is None or new_val == "" or new_val == "unknown" or new_val == "Inconnu":
                            continue
                update_fields.append(f)

            if not update_fields:
                product_id = existing["id"]
            else:
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
        release_connection(conn)


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
        release_connection(conn)


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
            SELECT p.id, p.name, p.brand, p.normalized_key, p.type_whey,
                   p.proteines_100g, p.bcaa_per_100g_prot, p.leucine_g,
                   p.isoleucine_g, p.valine_g, p.has_aminogram, p.mentions_bcaa,
                   p.ingredient_count, p.has_sucralose, p.has_acesulfame_k,
                   p.has_aspartame, p.has_artificial_flavors, p.has_thickeners,
                   p.has_colorants, p.origin_label, p.origin_confidence,
                   p.made_in_france, p.profil_suspect,
                   p.score_proteique, p.score_sante, p.score_global, p.score_final,
                   p.image_url, p.needs_review, p.created_at, p.updated_at,
                   bo.offer_prix, bo.offer_prix_par_kg, bo.offer_url,
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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


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
        release_connection(conn)


# ---------------------------------------------------------------------------
# Review functions
# ---------------------------------------------------------------------------

def get_product_by_id(product_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM products WHERE id = %s", (product_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        cur.close()
        release_connection(conn)


def create_review(product_id: int, user_id: int, rating: int, title: str = "",
                  comment: str = "", purchased_from: str = "") -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """INSERT INTO reviews (product_id, user_id, rating, title, comment, purchased_from)
               VALUES (%s, %s, %s, %s, %s, %s)
               RETURNING *""",
            (product_id, user_id, rating, title, comment, purchased_from),
        )
        review = dict(cur.fetchone())
        conn.commit()
        return review
    except Exception:
        conn.rollback()
        return None
    finally:
        cur.close()
        release_connection(conn)


def get_reviews_for_product(product_id: int, include_hidden: bool = False) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        if include_hidden:
            cur.execute(
                """SELECT r.*, u.display_name
                   FROM reviews r
                   JOIN users u ON r.user_id = u.id
                   WHERE r.product_id = %s
                   ORDER BY r.created_at DESC""",
                (product_id,),
            )
        else:
            cur.execute(
                """SELECT r.*, u.display_name
                   FROM reviews r
                   JOIN users u ON r.user_id = u.id
                   WHERE r.product_id = %s AND r.is_hidden = FALSE
                   ORDER BY r.created_at DESC""",
                (product_id,),
            )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        release_connection(conn)


def get_average_rating(product_id: int) -> dict:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT COALESCE(AVG(rating), 0) AS average, COUNT(*) AS count
               FROM reviews
               WHERE product_id = %s AND is_hidden = FALSE""",
            (product_id,),
        )
        row = cur.fetchone()
        return {
            "average": round(float(row["average"]), 1),
            "count": row["count"],
        }
    finally:
        cur.close()
        release_connection(conn)


def flag_review(review_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE reviews SET is_flagged = TRUE WHERE id = %s",
            (review_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        release_connection(conn)


def update_product_image(product_id: int, image_url: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE products SET image_url = %s, updated_at = NOW() WHERE id = %s AND (image_url IS NULL OR image_url = '')",
            (image_url, product_id),
        )
        conn.commit()
    finally:
        cur.close()
        release_connection(conn)


def get_flagged_reviews(limit: int = 50) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT r.*, u.display_name, p.name AS product_name
               FROM reviews r
               JOIN users u ON r.user_id = u.id
               JOIN products p ON r.product_id = p.id
               WHERE r.is_flagged = TRUE AND r.is_hidden = FALSE
               ORDER BY r.created_at DESC
               LIMIT %s""",
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        release_connection(conn)


def create_recommendation(product_id: int, user_id: int, usage_context: str,
                          level: str, pros: str, cons: str, comment: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO recommendations (product_id, user_id, usage_context, level, pros, cons, comment)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (product_id, user_id, usage_context, level, pros, cons, comment),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def get_recommendations_for_product(product_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT r.*, u.display_name
               FROM recommendations r
               JOIN users u ON r.user_id = u.id
               WHERE r.product_id = %s AND r.is_hidden = FALSE
               ORDER BY r.created_at DESC""",
            (product_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_top_products(limit: int = 5) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT p.id, p.name, p.brand, p.proteines_100g, p.score_final, 
                      p.score_proteique, p.score_sante, p.type_whey, p.image_url,
                      p.bcaa_per_100g_prot, p.leucine_g, p.origin_label
               FROM products p
               WHERE p.score_final IS NOT NULL
               ORDER BY p.score_final DESC
               LIMIT %s""",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_products_by_ids(product_ids: list[int]) -> list[dict]:
    if not product_ids:
        return []
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        placeholders = ",".join(["%s"] * len(product_ids))
        cur.execute(
            f"""WITH best_offers AS (
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
            WHERE p.id IN ({placeholders})""",
            product_ids,
        )
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        release_connection(conn)


def hide_review(review_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE reviews SET is_hidden = TRUE WHERE id = %s",
            (review_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        release_connection(conn)


def get_data_quality_stats() -> dict:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM products")
        total = cur.fetchone()[0]

        checks = {
            "no_protein": "SELECT COUNT(*) FROM products WHERE proteines_100g IS NULL OR proteines_100g = 0",
            "no_score": "SELECT COUNT(*) FROM products WHERE score_final IS NULL",
            "no_ingredients": "SELECT COUNT(*) FROM products WHERE ingredients IS NULL OR ingredients = '[]'",
            "no_bcaa": "SELECT COUNT(*) FROM products WHERE bcaa_per_100g_prot IS NULL",
            "no_amino": "SELECT COUNT(*) FROM products WHERE amino_profile IS NULL",
            "no_kcal": "SELECT COUNT(*) FROM products WHERE kcal_per_100g IS NULL",
            "no_image": "SELECT COUNT(*) FROM products WHERE image_url IS NULL OR image_url = ''",
        }
        results = {"total": total}
        for key, q in checks.items():
            cur.execute(q)
            results[key] = cur.fetchone()[0]

        results["complete"] = total - results["no_score"]
        return results
    finally:
        cur.close()
        release_connection(conn)


_BAD_PRODUCT_NAME_PATTERNS = [
    r"^.{0,3}$",
    r"(?i)achat\s*/\s*vente",
    r"(?i)^prot[ée]ines?\s*\|",
    r"(?i)^whey\s+prot[ée]ine?\s*$",
    r"(?i)nutrition\s+sportive\s*$",
    r"(?i)^\s*accueil\s*$",
    r"(?i)^\s*home\s*$",
]

def cleanup_catalog() -> dict:
    import re as _re
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT p.id, p.name, p.brand, p.proteines_100g, p.ingredients, p.score_final
            FROM products p
            LEFT JOIN offers o ON p.id = o.product_id AND o.is_active = TRUE
            GROUP BY p.id
            HAVING COUNT(o.id) = 0
               OR (p.proteines_100g IS NULL AND p.ingredients IS NULL AND p.score_final IS NULL)
        """)
        candidates = cur.fetchall()

        removed_ids = []
        removed_names = []

        for row in candidates:
            pid = row["id"]
            name = row.get("name", "") or ""
            prot = row.get("proteines_100g")
            ingr = row.get("ingredients")
            score = row.get("score_final")

            is_bad_name = any(_re.search(p, name) for p in _BAD_PRODUCT_NAME_PATTERNS)
            is_triple_miss = prot is None and (ingr is None or ingr == "[]") and score is None

            if is_bad_name or is_triple_miss:
                removed_ids.append(pid)
                removed_names.append(name[:80])

        if removed_ids:
            placeholders = ", ".join(["%s"] * len(removed_ids))
            cur.execute(f"DELETE FROM offers WHERE product_id IN ({placeholders})", removed_ids)
            cur.execute(f"DELETE FROM reviews WHERE product_id IN ({placeholders})", removed_ids)
            cur.execute(f"DELETE FROM products WHERE id IN ({placeholders})", removed_ids)
            conn.commit()

        return {
            "removed_count": len(removed_ids),
            "removed_names": removed_names,
        }
    finally:
        cur.close()
        release_connection(conn)


def record_price_snapshot(product_id: int, prix: float, prix_par_kg: float, merchant: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO price_history (product_id, prix, prix_par_kg, merchant)
               VALUES (%s, %s, %s, %s)""",
            (product_id, prix, prix_par_kg, merchant),
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        release_connection(conn)


def get_price_history(product_id: int, limit: int = 90) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT prix, prix_par_kg, merchant, recorded_at
               FROM price_history
               WHERE product_id = %s
               ORDER BY recorded_at ASC
               LIMIT %s""",
            (product_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def create_price_alert(user_id: int, product_id: int, target_price: float) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO price_alerts (user_id, product_id, target_price)
               VALUES (%s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (user_id, product_id, target_price),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def get_user_price_alerts(user_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT pa.*, p.name AS product_name, p.brand AS product_brand
               FROM price_alerts pa
               JOIN products p ON pa.product_id = p.id
               WHERE pa.user_id = %s AND pa.is_active = TRUE
               ORDER BY pa.created_at DESC""",
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def delete_price_alert(alert_id: int, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE price_alerts SET is_active = FALSE WHERE id = %s AND user_id = %s",
            (alert_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        release_connection(conn)


def check_and_trigger_alerts(product_id: int, current_prix_par_kg: float):
    if current_prix_par_kg is None:
        return
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT pa.id, pa.user_id, pa.target_price, p.name
               FROM price_alerts pa
               JOIN products p ON pa.product_id = p.id
               WHERE pa.product_id = %s AND pa.is_active = TRUE AND pa.target_price >= %s""",
            (product_id, current_prix_par_kg),
        )
        triggered = cur.fetchall()
        for alert in triggered:
            cur.execute(
                """INSERT INTO notifications (user_id, message, link_page, link_product_id)
                   VALUES (%s, %s, %s, %s)""",
                (
                    alert["user_id"],
                    f"Le prix de {alert['name']} est descendu a {current_prix_par_kg:.0f} EUR/kg (votre alerte: {alert['target_price']:.0f} EUR/kg)",
                    "product",
                    product_id,
                ),
            )
            cur.execute(
                "UPDATE price_alerts SET is_active = FALSE WHERE id = %s",
                (alert["id"],),
            )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        release_connection(conn)


def get_user_notifications(user_id: int, limit: int = 20) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT * FROM notifications
               WHERE user_id = %s
               ORDER BY created_at DESC
               LIMIT %s""",
            (user_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_unread_notification_count(user_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id = %s AND is_read = FALSE",
            (user_id,),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()
        release_connection(conn)


def mark_notifications_read(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE notifications SET is_read = TRUE WHERE user_id = %s AND is_read = FALSE",
            (user_id,),
        )
        conn.commit()
    finally:
        cur.close()
        release_connection(conn)


def get_user_preferences(user_id: int) -> dict:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT * FROM user_preferences WHERE user_id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        return {"weight_protein": 50, "weight_health": 35, "weight_price": 15, "email_alerts": False}
    finally:
        cur.close()
        release_connection(conn)


def save_user_preferences(user_id: int, weight_protein: float, weight_health: float, weight_price: float) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO user_preferences (user_id, weight_protein, weight_health, weight_price, updated_at)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (user_id)
               DO UPDATE SET weight_protein = %s, weight_health = %s, weight_price = %s, updated_at = NOW()""",
            (user_id, weight_protein, weight_health, weight_price,
             weight_protein, weight_health, weight_price),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def ensure_product_images_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_images (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                image_url TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_product_images_product_id ON product_images(product_id)")
        conn.commit()
    finally:
        cur.close()
        release_connection(conn)


def add_product_image(product_id: int, image_url: str, sort_order: int = 0) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM product_images WHERE product_id = %s AND image_url = %s", (product_id, image_url))
        if cur.fetchone():
            return False
        cur.execute(
            "INSERT INTO product_images (product_id, image_url, sort_order) VALUES (%s, %s, %s)",
            (product_id, image_url, sort_order)
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def get_product_images(product_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT id, image_url, sort_order FROM product_images WHERE product_id = %s ORDER BY sort_order, id",
            (product_id,)
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def ensure_user_favorites_table():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_favorites (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, product_id)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_favorites_user ON user_favorites(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_favorites_product ON user_favorites(product_id)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_badges (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                badge_type VARCHAR(50) NOT NULL,
                earned_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, badge_type)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_badges_user ON user_badges(user_id)")
        cur.execute("ALTER TABLE user_preferences ADD COLUMN IF NOT EXISTS email_alerts BOOLEAN DEFAULT FALSE")

        conn.commit()
    finally:
        cur.close()
        release_connection(conn)


def toggle_favorite(user_id: int, product_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM user_favorites WHERE user_id = %s AND product_id = %s", (user_id, product_id))
        existing = cur.fetchone()
        if existing:
            cur.execute("DELETE FROM user_favorites WHERE id = %s", (existing[0],))
            conn.commit()
            return False
        else:
            cur.execute("INSERT INTO user_favorites (user_id, product_id) VALUES (%s, %s)", (user_id, product_id))
            conn.commit()
            return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def is_favorite(user_id: int, product_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM user_favorites WHERE user_id = %s AND product_id = %s", (user_id, product_id))
        return cur.fetchone() is not None
    finally:
        cur.close()
        release_connection(conn)


def get_user_favorites(user_id: int) -> list[int]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT product_id FROM user_favorites WHERE user_id = %s ORDER BY created_at DESC", (user_id,))
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_user_favorites_count(user_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM user_favorites WHERE user_id = %s", (user_id,))
        return cur.fetchone()[0]
    finally:
        cur.close()
        release_connection(conn)


def delete_product_image(image_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM product_images WHERE id = %s", (image_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def get_user_badges(user_id: int) -> list:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT badge_type, earned_at FROM user_badges WHERE user_id = %s ORDER BY earned_at", (user_id,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def award_badge(user_id: int, badge_type: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO user_badges (user_id, badge_type) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, badge_type))
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()
        release_connection(conn)


def check_and_award_badges(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM reviews WHERE user_id = %s AND is_hidden = FALSE", (user_id,))
        review_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM recommendations WHERE user_id = %s AND is_hidden = FALSE", (user_id,))
        reco_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM price_alerts WHERE user_id = %s", (user_id,))
        alert_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM user_favorites WHERE user_id = %s", (user_id,))
        fav_count = cur.fetchone()[0]
    finally:
        cur.close()
        release_connection(conn)

    if review_count >= 1:
        award_badge(user_id, "first_review")
    if review_count >= 10:
        award_badge(user_id, "top_reviewer")
    if reco_count >= 3:
        award_badge(user_id, "community_helper")
    if alert_count >= 3:
        award_badge(user_id, "price_hunter")
    if fav_count >= 5:
        award_badge(user_id, "collector")


def get_recent_products(days: int = 30, limit: int = 10) -> list:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT p.*, o.prix_par_kg AS offer_prix_par_kg, o.prix AS offer_prix, o.poids_kg AS offer_poids_kg
            FROM products p
            LEFT JOIN LATERAL (
                SELECT prix_par_kg, prix, poids_kg FROM offers WHERE product_id = p.id AND is_active = TRUE
                ORDER BY confidence DESC LIMIT 1
            ) o ON TRUE
            WHERE p.created_at >= NOW() - make_interval(days => %s)
              AND p.score_final IS NOT NULL
            ORDER BY p.created_at DESC
            LIMIT %s
        """, (days, limit))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)


def get_anomalous_products() -> dict:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    results = {}
    try:
        cur.execute("SELECT id, name, brand, proteines_100g FROM products WHERE proteines_100g > 95")
        results["high_protein"] = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT id, name, brand FROM products WHERE name LIKE '%%|%%' OR name LIKE '%%Acheter%%'")
        results["bad_names"] = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT p.id, p.name, p.brand FROM products p
            LEFT JOIN offers o ON p.id = o.product_id AND o.is_active = TRUE
            WHERE o.id IS NULL AND p.score_final IS NOT NULL
        """)
        results["no_price"] = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT name, brand, COUNT(*) as cnt FROM products
            GROUP BY name, brand HAVING COUNT(*) > 1
        """)
        results["duplicates"] = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT id, name, brand FROM products WHERE image_url IS NULL OR image_url = ''")
        results["no_image"] = [dict(r) for r in cur.fetchall()]

        return results
    finally:
        cur.close()
        release_connection(conn)


def save_email_alert_preference(user_id: int, enabled: bool):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO user_preferences (user_id, email_alerts) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET email_alerts = %s
        """, (user_id, enabled, enabled))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        release_connection(conn)


def get_incomplete_products_for_rescrape(limit: int = 50) -> list:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT DISTINCT ON (p.id)
                   p.id, p.name, p.brand, p.proteines_100g, p.ingredients, p.image_url, p.score_final,
                   o.url AS offer_url, o.merchant
            FROM products p
            JOIN offers o ON p.id = o.product_id AND o.is_active = TRUE
            WHERE (p.proteines_100g IS NULL OR p.proteines_100g = 0)
               OR (p.ingredients IS NULL OR p.ingredients = '[]')
               OR (p.image_url IS NULL OR p.image_url = '')
               OR p.score_final IS NULL
            ORDER BY p.id, o.confidence DESC
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_connection(conn)
