import streamlit as st
import pandas as pd
import os
import base64
import html as html_module
from datetime import datetime

from scraper import (
    scrape_products, extract_product_data, BraveAPIError, SEARCH_QUERIES,
    run_discovery, run_refresh, get_discovery_stats_from_db,
    SEED_BRANDS, BLOCK_DOMAINS, MAX_PER_DOMAIN,
)
from scoring import calculate_price_score, calculate_final_score_10, calculate_price_score_10
from db import (
    init_db, create_user, get_user_by_email,
    save_scan, get_user_scans, get_scan_items,
    get_all_products, get_catalog_stats, get_pipeline_runs,
)
from auth import hash_password, verify_password
from page_validator import validate_url_debug, is_whey_product_page
from resolver import resolve_url_debug

init_db()

def get_logo_base64():
    logo_path = os.path.join(os.path.dirname(__file__), "static", "logo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

LOGO_B64 = get_logo_base64()

def render_page_header(title):
    st.html(f"""
    <div class='page-header'>
        <span class='page-header-title'>{html_module.escape(title)}</span>
    </div>
    """)

st.set_page_config(
    page_title="ProteinScan - Comparateur Whey",
    page_icon="🧪",
    layout="wide",
)

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="st-"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}

.stApp {
    background: radial-gradient(ellipse at 50% 25%, #1a1b3b 0%, #121732 25%, #0e1024 50%, #090c1b 80%, #080a18 100%) !important;
}

.stMainBlockContainer {
    background: transparent !important;
    border-radius: 20px !important;
    margin: 8px !important;
    padding: 24px 32px !important;
    border: none !important;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0e1028 0%, #0c0f22 100%) !important;
    border-right: 1px solid rgba(74, 158, 237, 0.15) !important;
}

section[data-testid="stSidebar"] .stMarkdown h1,
section[data-testid="stSidebar"] .stMarkdown h2,
section[data-testid="stSidebar"] .stMarkdown h3 {
    color: #e2e8f0 !important;
}

div[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(17, 24, 39, 0.8), rgba(15, 20, 35, 0.6)) !important;
    border: 1px solid rgba(74, 158, 237, 0.15) !important;
    border-radius: 12px !important;
    padding: 16px 20px !important;
}

div[data-testid="stMetric"] label {
    color: #8b9dc3 !important;
    font-size: 0.8em !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #e2e8f0 !important;
    font-weight: 700 !important;
}

.stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 0.9em !important;
    transition: all 0.3s ease !important;
    border: 1px solid rgba(74, 158, 237, 0.3) !important;
    background: linear-gradient(135deg, rgba(74, 158, 237, 0.15), rgba(59, 130, 246, 0.1)) !important;
    color: #4a9eed !important;
}

.stButton > button:hover {
    background: linear-gradient(135deg, rgba(74, 158, 237, 0.3), rgba(59, 130, 246, 0.2)) !important;
    border-color: rgba(74, 158, 237, 0.5) !important;
    box-shadow: 0 4px 15px rgba(74, 158, 237, 0.2) !important;
    transform: translateY(-1px) !important;
}

.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #2563eb, #3b82f6) !important;
    color: white !important;
    border: 1px solid rgba(59, 130, 246, 0.5) !important;
    box-shadow: 0 2px 10px rgba(37, 99, 235, 0.3) !important;
}

.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #1d4ed8, #2563eb) !important;
    box-shadow: 0 4px 20px rgba(37, 99, 235, 0.4) !important;
}

div[data-testid="stForm"] {
    background: linear-gradient(135deg, rgba(12, 17, 30, 0.7), rgba(10, 14, 24, 0.5)) !important;
    border: 1px solid rgba(74, 158, 237, 0.18) !important;
    border-radius: 14px !important;
    padding: 28px 24px !important;
}

.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div {
    background: rgba(8, 12, 22, 0.8) !important;
    border: 1px solid rgba(74, 158, 237, 0.22) !important;
    border-radius: 10px !important;
    color: #e2e8f0 !important;
    padding: 10px 14px !important;
}

.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #4a9eed !important;
    box-shadow: 0 0 0 2px rgba(74, 158, 237, 0.15) !important;
}

.stTextInput label,
.stTextArea label,
.stSelectbox label {
    color: #8b9dc3 !important;
    font-size: 0.85em !important;
    font-weight: 500 !important;
}

div[data-testid="stExpander"] {
    background: rgba(17, 24, 39, 0.5) !important;
    border: 1px solid rgba(74, 158, 237, 0.12) !important;
    border-radius: 12px !important;
}

.stDataFrame {
    border-radius: 12px !important;
    overflow: hidden !important;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 8px !important;
    background: rgba(17, 24, 39, 0.4) !important;
    border-radius: 12px !important;
    padding: 4px !important;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    color: #8b9dc3 !important;
    font-weight: 500 !important;
}

.stTabs [aria-selected="true"] {
    background: rgba(74, 158, 237, 0.2) !important;
    color: #4a9eed !important;
}

hr {
    border-color: rgba(74, 158, 237, 0.12) !important;
}

.stProgress > div > div > div {
    background: linear-gradient(90deg, #2563eb, #4a9eed) !important;
    border-radius: 10px !important;
}

div[data-testid="stSlider"] > div > div {
    color: #4a9eed !important;
}

.stDownloadButton > button {
    background: linear-gradient(135deg, rgba(74, 158, 237, 0.12), rgba(59, 130, 246, 0.08)) !important;
    border: 1px solid rgba(74, 158, 237, 0.25) !important;
    color: #4a9eed !important;
    border-radius: 10px !important;
}

.stDownloadButton > button:hover {
    background: linear-gradient(135deg, rgba(74, 158, 237, 0.25), rgba(59, 130, 246, 0.15)) !important;
}

div.stAlert {
    border-radius: 10px !important;
}

.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 0 16px 0;
    margin-bottom: 8px;
    border-bottom: 1px solid rgba(74, 158, 237, 0.15);
}

.sidebar-logo-icon {
    font-size: 1.8em;
    background: linear-gradient(135deg, #2563eb, #4a9eed);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.sidebar-logo-text {
    font-size: 1.4em;
    font-weight: 800;
    color: #e2e8f0;
    letter-spacing: -0.5px;
}

.sidebar-user {
    background: linear-gradient(135deg, rgba(37, 99, 235, 0.1), rgba(74, 158, 237, 0.05));
    border: 1px solid rgba(74, 158, 237, 0.15);
    border-radius: 12px;
    padding: 14px 16px;
    margin: 12px 0 16px 0;
}

.sidebar-user-name {
    font-weight: 700;
    font-size: 1em;
    color: #e2e8f0;
}

.sidebar-user-email {
    font-size: 0.8em;
    color: #8b9dc3;
    margin-top: 2px;
}

.sidebar-user-plan {
    font-size: 0.78em;
    color: #4a9eed;
    font-weight: 600;
    margin-top: 6px;
    display: flex;
    align-items: center;
    gap: 4px;
}

.page-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
}

.page-header-icon {
    font-size: 1.6em;
    background: linear-gradient(135deg, #2563eb, #4a9eed);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.page-header-title {
    font-size: 1.8em;
    font-weight: 800;
    color: #e2e8f0;
    letter-spacing: -0.5px;
}

.page-subtitle {
    color: #8b9dc3;
    font-size: 0.95em;
    margin-bottom: 20px;
}

.stat-card {
    background: linear-gradient(135deg, rgba(17, 24, 39, 0.8), rgba(15, 20, 35, 0.5));
    border: 1px solid rgba(74, 158, 237, 0.15);
    border-radius: 14px;
    padding: 20px 24px;
    margin-bottom: 16px;
}

.stat-card-label {
    font-size: 0.75em;
    color: #8b9dc3;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
}

.stat-card-value {
    font-size: 1.8em;
    font-weight: 800;
    color: #e2e8f0;
}

.scan-row {
    background: linear-gradient(135deg, rgba(17, 24, 39, 0.6), rgba(15, 20, 35, 0.3));
    border: 1px solid rgba(74, 158, 237, 0.1);
    border-radius: 12px;
    padding: 16px 20px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
}

.scan-row-date {
    color: #e2e8f0;
    font-size: 0.9em;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 8px;
}

.scan-row-count {
    color: #f59e0b;
    font-size: 0.9em;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 6px;
}

.login-container {
    max-width: 900px;
    margin: 0 auto;
    padding-top: 40px;
}

.login-header {
    text-align: center;
    margin-bottom: 40px;
}

.login-logo {
    font-size: 3em;
    margin-bottom: 8px;
    background: linear-gradient(135deg, #2563eb, #4a9eed);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.login-title {
    font-size: 2.2em;
    font-weight: 800;
    color: #e2e8f0;
    margin-bottom: 4px;
}

.login-subtitle {
    font-size: 1em;
    color: #8b9dc3;
}

.login-plans {
    text-align: center;
    margin-top: 30px;
    padding: 16px 24px;
    background: linear-gradient(135deg, rgba(17, 24, 39, 0.5), rgba(15, 20, 35, 0.3));
    border: 1px solid rgba(74, 158, 237, 0.12);
    border-radius: 12px;
    color: #8b9dc3;
    font-size: 0.9em;
}

.ps-card {
    border: 1px solid rgba(74, 158, 237, 0.12);
    border-radius: 14px;
    padding: 18px 22px;
    margin-bottom: 12px;
    background: linear-gradient(135deg, rgba(17, 24, 39, 0.7), rgba(15, 20, 35, 0.4));
    transition: all 0.3s ease;
}
.ps-card:hover {
    border-color: rgba(74, 158, 237, 0.25);
    box-shadow: 0 4px 20px rgba(74, 158, 237, 0.08);
}
.ps-rank {
    font-size: 1.6em;
    font-weight: 800;
    text-align: center;
    line-height: 1.1;
}
.ps-stars {
    color: #f59e0b;
    font-size: 1.3em;
    letter-spacing: 1px;
}
.ps-stars-sm {
    color: #f59e0b;
    font-size: 0.95em;
}
.ps-score-num {
    font-size: 0.85em;
    color: #8b9dc3;
    margin-left: 4px;
}
.ps-title {
    font-size: 1.1em;
    font-weight: 700;
    margin-bottom: 2px;
    line-height: 1.3;
    color: #e2e8f0;
}
.ps-brand {
    font-size: 0.85em;
    color: #8b9dc3;
}
.ps-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.78em;
    font-weight: 600;
    margin: 2px 3px;
    line-height: 1.5;
}
.ps-badge-green { background: rgba(46,204,113,0.15); color: #34d399; border: 1px solid rgba(46,204,113,0.25); }
.ps-badge-blue { background: rgba(74,158,237,0.15); color: #4a9eed; border: 1px solid rgba(74,158,237,0.25); }
.ps-badge-gold { background: rgba(245,158,11,0.15); color: #f59e0b; border: 1px solid rgba(245,158,11,0.25); }
.ps-badge-red { background: rgba(239,68,68,0.15); color: #ef4444; border: 1px solid rgba(239,68,68,0.25); }
.ps-badge-gray { background: rgba(148,163,184,0.12); color: #94a3b8; border: 1px solid rgba(148,163,184,0.2); }
.ps-badge-purple { background: rgba(168,85,247,0.15); color: #a855f7; border: 1px solid rgba(168,85,247,0.25); }
.ps-badge-orange { background: rgba(251,146,60,0.15); color: #fb923c; border: 1px solid rgba(251,146,60,0.25); }
.ps-badge-top {
    background: linear-gradient(135deg, rgba(245,158,11,0.25), rgba(251,146,60,0.25));
    color: #f59e0b;
    border: 2px solid rgba(245,158,11,0.5);
    font-weight: 800;
    font-size: 0.85em;
    letter-spacing: 0.5px;
    padding: 3px 12px;
    animation: glow 2s ease-in-out infinite alternate;
}
@keyframes glow {
    from { box-shadow: 0 0 3px rgba(245,158,11,0.2); }
    to { box-shadow: 0 0 8px rgba(245,158,11,0.4); }
}
.ps-badge-transp { background: rgba(148,163,184,0.08); color: #94a3b8; border: 1px dashed rgba(148,163,184,0.3); }
.ps-metrics {
    display: flex;
    gap: 18px;
    flex-wrap: wrap;
    margin: 8px 0;
}
.ps-metric {
    min-width: 70px;
}
.ps-metric-label {
    font-size: 0.68em;
    color: #8b9dc3;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.ps-metric-val {
    font-size: 1.3em;
    font-weight: 700;
    color: #e2e8f0;
}
.ps-quality {
    font-size: 0.82em;
    color: #94a3b8;
    margin-top: 6px;
    line-height: 1.6;
}
.ps-why {
    font-size: 0.78em;
    color: #8b9dc3;
    font-style: italic;
    margin-top: 4px;
    padding: 4px 8px;
    background: rgba(74, 158, 237, 0.06);
    border-radius: 6px;
    display: inline-block;
}
.ps-sub-scores {
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
    margin-top: 8px;
}
.ps-sub-score {
    text-align: center;
    min-width: 65px;
}
.ps-sub-label {
    font-size: 0.65em;
    color: #8b9dc3;
    text-transform: uppercase;
}
.ps-link {
    font-size: 0.82em;
    color: #4a9eed;
    text-decoration: none;
}
.ps-link:hover {
    text-decoration: underline;
    color: #60a5fa;
}
.ps-score-big {
    font-size: 1.5em;
    font-weight: 800;
}

.upgrade-btn {
    display: inline-block;
    padding: 8px 20px;
    background: linear-gradient(135deg, rgba(74, 158, 237, 0.15), rgba(59, 130, 246, 0.1));
    border: 1px solid rgba(74, 158, 237, 0.3);
    border-radius: 10px;
    color: #4a9eed;
    font-weight: 600;
    font-size: 0.85em;
    text-decoration: none;
    cursor: pointer;
}

.section-title {
    font-size: 1.15em;
    font-weight: 700;
    color: #e2e8f0;
    margin: 16px 0 12px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
</style>
"""

CARD_CSS = GLOBAL_CSS

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

api_key = os.environ.get("BRAVE_API_KEY", "") or os.environ.get("BRAVE_SEARCH_API_KEY", "")

for key in ["user", "page", "view_scan_id"]:
    if key not in st.session_state:
        st.session_state[key] = None
if "page" not in st.session_state or st.session_state.page is None:
    st.session_state.page = "login"


def logout():
    st.session_state.user = None
    st.session_state.page = "login"
    st.session_state.view_scan_id = None


def is_valid(val):
    return val is not None and not (isinstance(val, float) and pd.isna(val))


def score_to_stars_10(score):
    if not is_valid(score):
        return "☆☆☆☆☆", "N/A"
    s = max(0, min(10, score))
    stars_5 = s / 2.0
    full = int(stars_5)
    half = 1 if (stars_5 - full) >= 0.3 else 0
    empty = 5 - full - half
    star_str = "★" * full + ("½" if half else "") + "☆" * empty
    return star_str, f"{s:.1f}/10"


def score_color_10(score):
    if not is_valid(score):
        return "#8b9dc3"
    if score >= 7:
        return "#34d399"
    if score >= 4:
        return "#f59e0b"
    return "#ef4444"


def quality_label(score):
    if not is_valid(score):
        return "N/A"
    if score >= 9:
        return "Excellent"
    if score >= 7:
        return "Tres bien"
    if score >= 5:
        return "Correct"
    if score >= 3:
        return "Moyen"
    return "Faible"


def get_whey_badge(wtype):
    wt = (wtype or "unknown").lower()
    badges = {
        "native": ("🏆 Native", "ps-badge-gold"),
        "isolate": ("⭐ Isolate", "ps-badge-blue"),
        "hydrolysate": ("🔬 Hydrolysate", "ps-badge-purple"),
        "concentrate": ("📦 Concentrate", "ps-badge-gray"),
    }
    label, cls = badges.get(wt, ("❓ Inconnu", "ps-badge-gray"))
    return f"<span class='ps-badge {cls}'>{label}</span>"


def get_origin_badge(origin_label):
    ol = (origin_label or "Inconnu")
    if ol == "France":
        return "<span class='ps-badge ps-badge-green'>🇫🇷 France</span>"
    if ol == "EU":
        return "<span class='ps-badge ps-badge-blue'>🇪🇺 Union Europeenne</span>"
    return "<span class='ps-badge ps-badge-gray'>❓ Origine inconnue</span>"


def get_sweetener_badges(has_sucr, has_ace, has_asp):
    parts = []
    if has_sucr:
        parts.append("Sucralose")
    if has_ace:
        parts.append("Ace-K")
    if has_asp:
        parts.append("Aspartame")
    if parts:
        return f"<span class='ps-badge ps-badge-red'>⚠️ {', '.join(parts)}</span>"
    return "<span class='ps-badge ps-badge-green'>✅ Sans edulcorant</span>"


def get_additive_badges(has_flavors, has_thick, has_color):
    badges = []
    if has_flavors:
        badges.append("<span class='ps-badge ps-badge-orange'>🧪 Aromes artificiels</span>")
    if has_thick:
        badges.append("<span class='ps-badge ps-badge-orange'>🫗 Epaississants</span>")
    if has_color:
        badges.append("<span class='ps-badge ps-badge-red'>🎨 Colorants</span>")
    return " ".join(badges)


def build_why_text(row):
    reasons = []

    prot = row.get("proteines_100g")
    if is_valid(prot):
        if prot >= 90:
            reasons.append(f"tres pure ({prot:.0f}g)")
        elif prot > 85:
            reasons.append(f"haute teneur ({prot:.0f}g)")
        elif prot >= 80:
            reasons.append(f"bonne teneur ({prot:.0f}g)")
        elif prot >= 75:
            reasons.append(f"teneur correcte ({prot:.0f}g)")
        else:
            reasons.append(f"teneur faible ({prot:.0f}g)")

    leucine = row.get("leucine_g")
    if is_valid(leucine):
        if leucine >= 10.5:
            reasons.append(f"leucine elevee ({leucine:.1f}g)")
        elif leucine >= 8:
            reasons.append(f"leucine correcte ({leucine:.1f}g)")
    else:
        reasons.append("leucine: non trouvee")

    bcaa = row.get("bcaa_per_100g_prot")
    if is_valid(bcaa):
        if bcaa > 24:
            reasons.append(f"BCAA excellent ({bcaa:.0f}g)")
        elif bcaa >= 20:
            reasons.append(f"BCAA correct ({bcaa:.0f}g)")
        else:
            reasons.append(f"BCAA faible ({bcaa:.0f}g)")
    else:
        reasons.append("BCAA: non trouves")

    if row.get("has_aminogram"):
        reasons.append("aminogramme present")

    if row.get("profil_suspect"):
        reasons.append("profil amino suspect")

    s_sante = row.get("score_sante")
    if is_valid(s_sante):
        if s_sante >= 9:
            reasons.append("composition tres propre")
        elif s_sante >= 7:
            reasons.append("composition correcte")
        elif s_sante < 5:
            reasons.append("composition a ameliorer")

    ingredient_count = row.get("ingredient_count")
    if is_valid(ingredient_count) and ingredient_count <= 6:
        reasons.append(f"compo courte ({ingredient_count} ingr.)")

    has_sucr = row.get("has_sucralose", False)
    has_ace = row.get("has_acesulfame_k", False)
    has_asp = row.get("has_aspartame", False)
    if not has_sucr and not has_ace and not has_asp:
        reasons.append("sans edulcorant")

    origin = row.get("origin_label", "Inconnu")
    if origin == "France":
        reasons.append("fabrication France")

    return " · ".join(reasons) if reasons else "Donnees insuffisantes"


def compute_top_qualite(row):
    sp = row.get("score_proteique")
    ss = row.get("score_sante")
    ic = row.get("ingredient_count")
    if not is_valid(sp) or not is_valid(ss):
        return False
    if sp >= 8.5 and ss >= 8.5:
        if is_valid(ic) and ic <= 9:
            return True
        if not is_valid(ic):
            return True
    return False


def compute_low_transparency(row):
    bcaa = row.get("bcaa_per_100g_prot")
    leucine = row.get("leucine_g")
    return not is_valid(bcaa) or not is_valid(leucine)


def get_score_final_for_row(row):
    sf = row.get("score_final")
    if is_valid(sf):
        return sf
    sp = row.get("score_proteique")
    ss = row.get("score_sante")
    pkg = row.get("prix_par_kg")
    if is_valid(sp) or is_valid(ss):
        result = calculate_final_score_10(
            score_proteique=sp,
            score_sante=ss,
            price_per_kg=pkg,
            protein_per_100g=row.get("proteines_100g"),
            leucine_g=row.get("leucine_g"),
            has_aminogram=row.get("has_aminogram", False),
            origin_label=row.get("origin_label", "Inconnu"),
            bcaa_missing=not is_valid(row.get("bcaa_per_100g_prot")),
            leucine_missing=not is_valid(row.get("leucine_g")),
            ingredient_count=row.get("ingredient_count"),
        )
        return result["score_final"]
    return row.get("score_global")


def render_product_card_v2(rank, row):
    s_global = row.get("score_global")
    s_proteique = row.get("score_proteique")
    s_sante = row.get("score_sante")
    s_final = get_score_final_for_row(row)

    import re as _re
    raw_nom = str(row.get("nom", "Produit inconnu") or "Produit inconnu")
    raw_nom = _re.sub(r'<[^>]+>', '', raw_nom).strip()
    raw_nom = html_module.unescape(raw_nom)
    nom = html_module.escape(raw_nom)
    if len(nom) > 100:
        nom = nom[:97] + "..."
    raw_marque = str(row.get("marque", "") or "")
    raw_marque = _re.sub(r'<[^>]+>', '', raw_marque).strip()
    raw_marque = html_module.unescape(raw_marque)
    marque = html_module.escape(raw_marque)
    url = row.get("url", "")

    prix = row.get("prix")
    prix_kg = row.get("prix_par_kg")
    prot = row.get("proteines_100g")
    poids = row.get("poids_kg")

    type_whey = row.get("type_whey", "unknown")
    origin_label = row.get("origin_label", "Inconnu")
    has_sucr = row.get("has_sucralose", False)
    has_ace = row.get("has_acesulfame_k", False)
    has_asp = row.get("has_aspartame", False)
    has_amino = row.get("has_aminogram", False)
    has_bcaa_flag = row.get("mentions_bcaa", False)
    has_flavors = row.get("has_artificial_flavors", False)
    has_thick = row.get("has_thickeners", False)
    has_color = row.get("has_colorants", False)
    ingredient_count = row.get("ingredient_count")
    bcaa_val = row.get("bcaa_per_100g_prot")
    leucine_val = row.get("leucine_g")
    profil_suspect = row.get("profil_suspect", False)
    protein_suspect = row.get("protein_suspect", False)
    protein_src = row.get("protein_source")

    is_top = compute_top_qualite(row)
    is_low_transp = compute_low_transparency(row)

    display_score = s_final if is_valid(s_final) else s_global
    color = score_color_10(display_score)
    stars_global, stars_num = score_to_stars_10(display_score)
    stars_prot, prot_score_num = score_to_stars_10(s_proteique)
    stars_sante, sante_num = score_to_stars_10(s_sante)

    prot_color = "#2ecc71" if is_valid(prot) and prot >= 80 else ("#f39c12" if is_valid(prot) and prot >= 70 else ("#e74c3c" if is_valid(prot) else "#888"))
    prix_kg_color = "#2ecc71" if is_valid(prix_kg) and prix_kg <= 30 else ("#f39c12" if is_valid(prix_kg) and prix_kg <= 50 else ("#e74c3c" if is_valid(prix_kg) else "#888"))

    if protein_suspect:
        prot_display = "<span style='color:#e74c3c;'>Suspecte</span>"
    elif is_valid(prot):
        prot_display = f"{prot:.1f}g"
    else:
        prot_display = "N/A"
    prix_kg_display = f"{prix_kg:.0f}€" if is_valid(prix_kg) else "N/A"
    prix_display = f"{prix:.2f}€" if is_valid(prix) else "N/A"
    poids_display = f"{poids:.2f}kg" if is_valid(poids) else "N/A"
    bcaa_display = f"{bcaa_val:.1f}g" if is_valid(bcaa_val) else "<span style='color:#999;font-size:0.85em;'>non trouve</span>"
    leucine_display = f"{leucine_val:.1f}g" if is_valid(leucine_val) else "<span style='color:#999;font-size:0.85em;'>non trouvee</span>"
    ingr_display = str(ingredient_count) if is_valid(ingredient_count) else "—"
    qual_label = quality_label(display_score)

    whey_badge = get_whey_badge(type_whey)
    origin_badge = get_origin_badge(origin_label)
    sweetener_badge = get_sweetener_badges(has_sucr, has_ace, has_asp)
    additive_badges = get_additive_badges(has_flavors, has_thick, has_color)

    extra_badges = ""
    if is_top:
        extra_badges += "<span class='ps-badge ps-badge-top'>🏅 TOP QUALITE</span>"
    if protein_suspect:
        extra_badges += "<span class='ps-badge ps-badge-red'>⚠️ Donnee nutrition suspecte</span>"
    elif profil_suspect:
        extra_badges += "<span class='ps-badge ps-badge-red'>⚠️ Profil suspect</span>"
    if has_amino:
        extra_badges += "<span class='ps-badge ps-badge-green'>🧬 Aminogramme</span>"
    elif has_bcaa_flag:
        extra_badges += "<span class='ps-badge ps-badge-blue'>💊 BCAA</span>"
    if is_valid(ingredient_count) and ingredient_count > 6:
        extra_badges += f"<span class='ps-badge ps-badge-orange'>📋 {ingredient_count} ingredients</span>"
    if is_low_transp:
        extra_badges += "<span class='ps-badge ps-badge-transp'>🔍 Transparence faible</span>"

    why_text = build_why_text(row)
    link_html = f"<a href='{url}' target='_blank' class='ps-link'>🔗 Voir le produit</a>" if url else ""

    final_display = f"{display_score:.1f}/10" if is_valid(display_score) else "N/A"

    price_score_10 = calculate_price_score_10(prix_kg)
    ps10_stars, ps10_num = score_to_stars_10(price_score_10)

    card_html = f"""
    <div class='ps-card'>
      <div style='display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start;'>

        <div style='flex-shrink:0;min-width:75px;text-align:center;'>
          <div class='ps-rank' style='color:{color};'>#{rank}</div>
          <div class='ps-stars'>{stars_global}</div>
          <div class='ps-score-big' style='color:{color};'>{final_display}</div>
          <div style='font-size:0.75em;color:#888;'>{qual_label}</div>
        </div>

        <div style='flex:1;min-width:250px;'>
          <div class='ps-title'>{nom}</div>
          <div class='ps-brand'>{marque}</div>

          <div style='margin:6px 0;'>
            {whey_badge} {origin_badge} {sweetener_badge} {additive_badges} {extra_badges}
          </div>

          <div class='ps-metrics'>
            <div class='ps-metric'>
              <div class='ps-metric-label'>Prot / 100g</div>
              <div class='ps-metric-val' style='color:{prot_color};'>{prot_display}</div>
            </div>
            <div class='ps-metric'>
              <div class='ps-metric-label'>BCAA / 100g prot</div>
              <div class='ps-metric-val' style='font-size:1em;'>{bcaa_display}</div>
            </div>
            <div class='ps-metric'>
              <div class='ps-metric-label'>Leucine / 100g prot</div>
              <div class='ps-metric-val' style='font-size:1em;'>{leucine_display}</div>
            </div>
            <div class='ps-metric'>
              <div class='ps-metric-label'>Prix / kg</div>
              <div class='ps-metric-val' style='color:{prix_kg_color};'>{prix_kg_display}</div>
            </div>
            <div class='ps-metric'>
              <div class='ps-metric-label'>Prix total</div>
              <div class='ps-metric-val' style='font-size:1em;'>{prix_display}</div>
            </div>
            <div class='ps-metric'>
              <div class='ps-metric-label'>Poids</div>
              <div class='ps-metric-val' style='font-size:1em;'>{poids_display}</div>
            </div>
          </div>

          <div class='ps-why'>💡 {why_text}</div>
          <div style='margin-top:6px;'>{link_html}</div>
        </div>

        <div style='min-width:200px;'>
          <div class='ps-sub-scores'>
            <div class='ps-sub-score'>
              <div class='ps-sub-label'>Proteique</div>
              <div class='ps-stars-sm'>{stars_prot}</div>
              <div style='font-size:0.9em;font-weight:700;color:{score_color_10(s_proteique)};'>{prot_score_num}</div>
            </div>
            <div class='ps-sub-score'>
              <div class='ps-sub-label'>Sante</div>
              <div class='ps-stars-sm'>{stars_sante}</div>
              <div style='font-size:0.9em;font-weight:700;color:{score_color_10(s_sante)};'>{sante_num}</div>
            </div>
            <div class='ps-sub-score'>
              <div class='ps-sub-label'>Prix</div>
              <div class='ps-stars-sm'>{ps10_stars}</div>
              <div style='font-size:0.9em;font-weight:700;color:{score_color_10(price_score_10)};'>{ps10_num}</div>
            </div>
          </div>

          <div class='ps-quality'>
            ✅ Type : {html_module.escape(type_whey.capitalize() if type_whey != 'unknown' else 'Non determine')}<br/>
            {'🇫🇷' if origin_label == 'France' else ('🇪🇺' if origin_label == 'EU' else '❓')} Origine : {html_module.escape(origin_label)}<br/>
            {'🚫' if (has_sucr or has_ace or has_asp) else '✅'} Edulcorants : {'Aucun' if not (has_sucr or has_ace or has_asp) else ', '.join(filter(None, ['Sucralose' if has_sucr else '', 'Ace-K' if has_ace else '', 'Aspartame' if has_asp else '']))}<br/>
            📋 Ingredients : {ingr_display}
          </div>
        </div>

      </div>
    </div>
    """
    st.html(CARD_CSS + card_html)


def render_results(products_data, is_dataframe=True):
    if is_dataframe:
        df = products_data.copy()
    else:
        df = pd.DataFrame(products_data)

    for col, default in [("origin_label", "Inconnu"), ("origin_confidence", 0.3),
                         ("ingredients", None), ("has_artificial_flavors", False),
                         ("has_thickeners", False), ("has_colorants", False),
                         ("ingredient_count", None), ("bcaa_per_100g_prot", None),
                         ("leucine_g", None), ("isoleucine_g", None), ("valine_g", None),
                         ("profil_suspect", False), ("score_proteique", None)]:
        if col not in df.columns:
            df[col] = default

    with_prot = df["proteines_100g"].dropna().shape[0] if "proteines_100g" in df.columns else 0
    with_prix = df["prix_par_kg"].dropna().shape[0] if "prix_par_kg" in df.columns else 0
    with_score = df["score_global"].dropna().shape[0] if "score_global" in df.columns else 0

    avg_prot = df["proteines_100g"].dropna().mean() if with_prot > 0 else None
    avg_prix_kg = df["prix_par_kg"].dropna().mean() if with_prix > 0 else None
    avg_global = df["score_global"].dropna().mean() if with_score > 0 else None

    st.subheader(f"🏆 Classement — {len(df)} produits analyses")

    col_s1, col_s2, col_s3, col_s4, col_s5 = st.columns(5)
    with col_s1:
        st.metric("Produits", len(df))
    with col_s2:
        st.metric("Donnees prot.", f"{with_prot}/{len(df)}")
    with col_s3:
        st.metric("Moy. prot/100g", f"{avg_prot:.1f}g" if avg_prot else "—")
    with col_s4:
        st.metric("Moy. prix/kg", f"{avg_prix_kg:.0f}€" if avg_prix_kg else "—")
    with col_s5:
        st.metric("Note moy.", f"{avg_global:.1f}/10" if avg_global else "—")

    st.divider()

    with st.expander("📐 Bareme & Legende", expanded=False):
        leg1, leg2, leg3 = st.columns(3)
        with leg1:
            st.markdown("""
**Note Proteique /10 (50% de la finale)**

| Critere | Bareme |
|---------|--------|
| **% proteines** (sur 5) | <70%=1, 70-75=2, 75-80=3, 80-85=4, >85%=5 |
| **BCAA/100g prot** (sur 3) | <20g=1, 20-24g=2, >24g=3 |
| **Leucine/100g prot** (sur 2) | <8g=0, 8-10g=1, >10g=2 |
| **Equilibre BCAA** | Ratio 2:1:1 attendu, malus -1 a -2 si suspect |

*Si BCAA/leucine non trouves : score neutre (pas 0)*
""")
        with leg2:
            st.markdown("""
**Note Sante /10 (35% de la finale)**

Commence a 10, puis malus :

| Critere | Malus |
|---------|-------|
| Edulcorant (sucralose, ace-K...) | -2 |
| Plusieurs edulcorants | -3 |
| Aromes artificiels | -1 |
| Epaississants (xanthane, carraghenane) | -1 |
| Colorants | -1 |
| 7-9 ingredients | -0.5 |
| 10-14 ingredients | -1.0 |
| 15-20 ingredients | -2.0 |
| >20 ingredients | -3.0 |
""")
        with leg3:
            st.markdown("""
**Note Finale /10**

= (proteique x 0.50) + (sante x 0.35) + (prix x 0.15)

**Bonus premium :**
- Proteines >= 90g/100g : +0.5
- Leucine >= 10.5g : +0.3
- Aminogramme present : +0.3
- Origine France : +0.2

**Badges :**
- 🏅 TOP QUALITE : prot >= 8.5 + sante >= 8.5 + <= 9 ingr.
- 🔍 Transparence faible : BCAA ou leucine non trouves

*Le prix n'est qu'un departage entre bons produits (15%)*
""")

    st.divider()

    fcol1, fcol2, fcol3, fcol4, fcol5, fcol6 = st.columns([2, 1.2, 1.2, 1.2, 1.2, 2])

    with fcol1:
        sort_option = st.selectbox(
            "Trier par",
            ["Note Finale", "Note Proteique", "Note Sante", "Prix/kg (croissant)", "Proteines/100g (decroissant)"],
            key="sort_option",
        )
    with fcol2:
        filter_top = st.toggle("TOP QUALITE", key="filter_top_qualite")
    with fcol3:
        filter_no_sweetener = st.toggle("Sans edulcorant", key="filter_no_sweet")
    with fcol4:
        filter_france = st.toggle("Fabrication France", key="filter_france")
    with fcol5:
        filter_clean = st.toggle("Compo propre (≥8)", key="filter_clean")
    with fcol6:
        type_options = ["Tous"] + sorted(set(
            str(t).capitalize() for t in df["type_whey"].dropna().unique() if t and str(t) != "unknown"
        ))
        type_options_full = type_options + (["Unknown"] if "unknown" in df["type_whey"].fillna("unknown").str.lower().unique() else [])
        filter_type = st.selectbox("Type de whey", type_options_full, key="filter_type")

    search_query = st.text_input("🔍 Rechercher un produit (nom ou marque)", key="search_query", placeholder="Ex: myprotein, isolate, native...")

    filtered_df = df.copy()

    if search_query:
        q = search_query.lower()
        filtered_df = filtered_df[
            filtered_df["nom"].fillna("").str.lower().str.contains(q, na=False) |
            filtered_df["marque"].fillna("").str.lower().str.contains(q, na=False)
        ]

    if filter_no_sweetener:
        filtered_df = filtered_df[
            ~filtered_df.get("has_sucralose", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool) &
            ~filtered_df.get("has_acesulfame_k", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool) &
            ~filtered_df.get("has_aspartame", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool)
        ]

    if filter_france:
        if "origin_label" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["origin_label"] == "France"]
        elif "made_in_france" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["made_in_france"].fillna(False).astype(bool)]

    if filter_clean:
        if "score_sante" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["score_sante"].fillna(0) >= 8]

    if filter_top:
        filtered_df = filtered_df[filtered_df.apply(lambda r: compute_top_qualite(r), axis=1)]

    if filter_type and filter_type != "Tous":
        filtered_df = filtered_df[filtered_df["type_whey"].fillna("unknown").str.lower() == filter_type.lower()]

    if "score_final" not in filtered_df.columns:
        filtered_df["score_final"] = filtered_df.apply(lambda r: get_score_final_for_row(r), axis=1)

    sort_map = {
        "Note Finale": ("score_final", False),
        "Note Proteique": ("score_proteique", False),
        "Note Sante": ("score_sante", False),
        "Prix/kg (croissant)": ("prix_par_kg", True),
        "Proteines/100g (decroissant)": ("proteines_100g", False),
    }
    sort_col, sort_asc = sort_map.get(sort_option, ("score_final", False))

    if sort_col in filtered_df.columns:
        sorted_df = filtered_df.sort_values(sort_col, ascending=sort_asc, na_position="last")
    else:
        sorted_df = filtered_df

    st.markdown(f"**{len(sorted_df)} produits affiches** sur {len(df)}")

    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        render_product_card_v2(rank, row)

    st.divider()
    st.subheader("📊 Tableau complet")

    display_cols = [
        "nom", "marque", "proteines_100g", "bcaa_per_100g_prot", "leucine_g",
        "prix_par_kg", "prix", "poids_kg",
        "type_whey", "origin_label", "ingredient_count",
        "has_sucralose", "has_acesulfame_k", "has_aspartame",
        "has_artificial_flavors", "has_thickeners", "has_colorants",
        "profil_suspect",
        "score_proteique", "score_sante", "score_final",
    ]
    existing_cols = [c for c in display_cols if c in sorted_df.columns]

    column_config = {
        "nom": st.column_config.TextColumn("Produit", width="large"),
        "marque": st.column_config.TextColumn("Marque"),
        "proteines_100g": st.column_config.NumberColumn("Prot/100g", format="%.1f g"),
        "bcaa_per_100g_prot": st.column_config.NumberColumn("BCAA/100g prot", format="%.1f g"),
        "leucine_g": st.column_config.NumberColumn("Leucine/100g prot", format="%.1f g"),
        "prix_par_kg": st.column_config.NumberColumn("Prix/kg", format="%.0f €"),
        "prix": st.column_config.NumberColumn("Prix", format="%.2f €"),
        "poids_kg": st.column_config.NumberColumn("Poids", format="%.2f kg"),
        "type_whey": st.column_config.TextColumn("Type"),
        "origin_label": st.column_config.TextColumn("Origine"),
        "ingredient_count": st.column_config.NumberColumn("Nb ingr."),
        "has_sucralose": st.column_config.CheckboxColumn("Sucralose"),
        "has_acesulfame_k": st.column_config.CheckboxColumn("Ace-K"),
        "has_aspartame": st.column_config.CheckboxColumn("Aspartame"),
        "has_artificial_flavors": st.column_config.CheckboxColumn("Aromes art."),
        "has_thickeners": st.column_config.CheckboxColumn("Epaississants"),
        "has_colorants": st.column_config.CheckboxColumn("Colorants"),
        "profil_suspect": st.column_config.CheckboxColumn("Profil suspect"),
        "score_proteique": st.column_config.ProgressColumn("Proteique /10", min_value=0, max_value=10),
        "score_sante": st.column_config.ProgressColumn("Sante /10", min_value=0, max_value=10),
        "score_final": st.column_config.ProgressColumn("Finale /10", min_value=0, max_value=10),
    }

    st.dataframe(
        sorted_df[existing_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("📥 Exporter")

    col_e1, col_e2 = st.columns(2)
    csv_data = sorted_df.to_csv(index=False, sep=";", encoding="utf-8-sig")

    with col_e1:
        st.download_button(
            label="Telecharger CSV",
            data=csv_data,
            file_name=f"proteines_whey_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col_e2:
        excel_path = "/tmp/export.xlsx"
        sorted_df.to_excel(excel_path, index=False, sheet_name="Produits")
        with open(excel_path, "rb") as f:
            st.download_button(
                label="Telecharger Excel",
                data=f.read(),
                file_name=f"proteines_whey_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )


# ── CATALOG CARD & RESULTS ──

def get_confidence_badge(confidence):
    if confidence is None or (isinstance(confidence, float) and pd.isna(confidence)):
        return "<span class='ps-badge ps-badge-gray'>🔍 Confiance: N/A</span>"
    try:
        conf = float(confidence)
        pct = int(conf * 100)
    except (ValueError, TypeError):
        return "<span class='ps-badge ps-badge-gray'>🔍 Confiance: N/A</span>"
    if conf >= 0.7:
        return f"<span class='ps-badge ps-badge-green'>🔍 Confiance: {pct}%</span>"
    if conf >= 0.4:
        return f"<span class='ps-badge ps-badge-gold'>🔍 Confiance: {pct}%</span>"
    return f"<span class='ps-badge ps-badge-red'>🔍 Confiance: {pct}%</span>"


def render_catalog_card(rank, product):
    mapped = {
        "nom": product.get("name", "Produit inconnu"),
        "marque": product.get("brand", ""),
        "prix": product.get("offer_prix"),
        "prix_par_kg": product.get("offer_prix_par_kg"),
        "url": product.get("offer_url", ""),
        "poids_kg": product.get("offer_poids_kg"),
        "proteines_100g": product.get("proteines_100g"),
        "type_whey": product.get("type_whey", "unknown"),
        "origin_label": product.get("origin_label", "Inconnu"),
        "has_sucralose": product.get("has_sucralose", False),
        "has_acesulfame_k": product.get("has_acesulfame_k", False),
        "has_aspartame": product.get("has_aspartame", False),
        "has_aminogram": product.get("has_aminogram", False),
        "mentions_bcaa": product.get("mentions_bcaa", False),
        "has_artificial_flavors": product.get("has_artificial_flavors", False),
        "has_thickeners": product.get("has_thickeners", False),
        "has_colorants": product.get("has_colorants", False),
        "ingredient_count": product.get("ingredient_count"),
        "bcaa_per_100g_prot": product.get("bcaa_per_100g_prot"),
        "leucine_g": product.get("leucine_g"),
        "isoleucine_g": product.get("isoleucine_g"),
        "valine_g": product.get("valine_g"),
        "profil_suspect": product.get("profil_suspect", False),
        "protein_suspect": product.get("protein_suspect", False),
        "protein_source": product.get("protein_source"),
        "score_proteique": product.get("score_proteique"),
        "score_sante": product.get("score_sante"),
        "score_global": product.get("score_global"),
        "score_final": product.get("score_final"),
    }

    render_product_card_v2(rank, mapped)

    confidence = product.get("offer_confidence")
    badge_html = get_confidence_badge(confidence)
    st.html(CARD_CSS + f"<div style='margin-top:-8px;margin-bottom:12px;padding-left:95px;'>{badge_html}</div>")


def render_catalog_results(products):
    mapped_products = []
    for p in products:
        mapped_products.append({
            "nom": p.get("name", "Produit inconnu"),
            "marque": p.get("brand", ""),
            "prix": p.get("offer_prix"),
            "prix_par_kg": p.get("offer_prix_par_kg"),
            "url": p.get("offer_url", ""),
            "poids_kg": p.get("offer_poids_kg"),
            "proteines_100g": p.get("proteines_100g"),
            "type_whey": p.get("type_whey", "unknown"),
            "origin_label": p.get("origin_label", "Inconnu"),
            "has_sucralose": p.get("has_sucralose", False),
            "has_acesulfame_k": p.get("has_acesulfame_k", False),
            "has_aspartame": p.get("has_aspartame", False),
            "has_aminogram": p.get("has_aminogram", False),
            "mentions_bcaa": p.get("mentions_bcaa", False),
            "has_artificial_flavors": p.get("has_artificial_flavors", False),
            "has_thickeners": p.get("has_thickeners", False),
            "has_colorants": p.get("has_colorants", False),
            "ingredient_count": p.get("ingredient_count"),
            "bcaa_per_100g_prot": p.get("bcaa_per_100g_prot"),
            "leucine_g": p.get("leucine_g"),
            "isoleucine_g": p.get("isoleucine_g"),
            "valine_g": p.get("valine_g"),
            "profil_suspect": p.get("profil_suspect", False),
            "protein_suspect": p.get("protein_suspect", False),
            "protein_source": p.get("protein_source"),
            "score_proteique": p.get("score_proteique"),
            "score_sante": p.get("score_sante"),
            "score_global": p.get("score_global"),
            "score_final": p.get("score_final"),
            "offer_confidence": p.get("offer_confidence"),
        })

    df = pd.DataFrame(mapped_products)

    for col, default in [("origin_label", "Inconnu"), ("has_artificial_flavors", False),
                         ("has_thickeners", False), ("has_colorants", False),
                         ("ingredient_count", None), ("bcaa_per_100g_prot", None),
                         ("leucine_g", None), ("isoleucine_g", None), ("valine_g", None),
                         ("profil_suspect", False), ("protein_suspect", False),
                         ("score_proteique", None)]:
        if col not in df.columns:
            df[col] = default

    with_prot = df["proteines_100g"].dropna().shape[0] if "proteines_100g" in df.columns else 0
    with_prix = df["prix_par_kg"].dropna().shape[0] if "prix_par_kg" in df.columns else 0
    with_score = df["score_global"].dropna().shape[0] if "score_global" in df.columns else 0

    avg_prot = df["proteines_100g"].dropna().mean() if with_prot > 0 else None
    avg_prix_kg = df["prix_par_kg"].dropna().mean() if with_prix > 0 else None
    avg_global = df["score_global"].dropna().mean() if with_score > 0 else None

    st.subheader(f"🏆 Catalogue — {len(df)} produits")

    col_s1, col_s2, col_s3, col_s4, col_s5 = st.columns(5)
    with col_s1:
        st.metric("Produits", len(df))
    with col_s2:
        st.metric("Donnees prot.", f"{with_prot}/{len(df)}")
    with col_s3:
        st.metric("Moy. prot/100g", f"{avg_prot:.1f}g" if avg_prot else "—")
    with col_s4:
        st.metric("Moy. prix/kg", f"{avg_prix_kg:.0f}€" if avg_prix_kg else "—")
    with col_s5:
        st.metric("Note moy.", f"{avg_global:.1f}/10" if avg_global else "—")

    st.divider()

    fcol1, fcol2, fcol3, fcol4, fcol5, fcol6 = st.columns([2, 1.2, 1.2, 1.2, 1.2, 2])

    with fcol1:
        sort_option = st.selectbox(
            "Trier par",
            ["Note Finale", "Note Proteique", "Note Sante", "Prix/kg (croissant)", "Proteines/100g (decroissant)"],
            key="cat_sort_option",
        )
    with fcol2:
        filter_top = st.toggle("TOP QUALITE", key="cat_filter_top_qualite")
    with fcol3:
        filter_no_sweetener = st.toggle("Sans edulcorant", key="cat_filter_no_sweet")
    with fcol4:
        filter_france = st.toggle("Fabrication France", key="cat_filter_france")
    with fcol5:
        filter_clean = st.toggle("Compo propre (≥8)", key="cat_filter_clean")
    with fcol6:
        type_options = ["Tous"] + sorted(set(
            str(t).capitalize() for t in df["type_whey"].dropna().unique() if t and str(t) != "unknown"
        ))
        type_options_full = type_options + (["Unknown"] if "unknown" in df["type_whey"].fillna("unknown").str.lower().unique() else [])
        filter_type = st.selectbox("Type de whey", type_options_full, key="cat_filter_type")

    search_query = st.text_input("🔍 Rechercher un produit (nom ou marque)", key="cat_search_query", placeholder="Ex: myprotein, isolate, native...")

    filtered_df = df.copy()

    if search_query:
        q = search_query.lower()
        filtered_df = filtered_df[
            filtered_df["nom"].fillna("").str.lower().str.contains(q, na=False) |
            filtered_df["marque"].fillna("").str.lower().str.contains(q, na=False)
        ]

    if filter_no_sweetener:
        filtered_df = filtered_df[
            ~filtered_df.get("has_sucralose", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool) &
            ~filtered_df.get("has_acesulfame_k", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool) &
            ~filtered_df.get("has_aspartame", pd.Series([False] * len(filtered_df))).fillna(False).astype(bool)
        ]

    if filter_france:
        if "origin_label" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["origin_label"] == "France"]

    if filter_clean:
        if "score_sante" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["score_sante"].fillna(0) >= 8]

    if filter_top:
        filtered_df = filtered_df[filtered_df.apply(lambda r: compute_top_qualite(r), axis=1)]

    if filter_type and filter_type != "Tous":
        filtered_df = filtered_df[filtered_df["type_whey"].fillna("unknown").str.lower() == filter_type.lower()]

    if "score_final" not in filtered_df.columns:
        filtered_df["score_final"] = filtered_df.apply(lambda r: get_score_final_for_row(r), axis=1)

    sort_map = {
        "Note Finale": ("score_final", False),
        "Note Proteique": ("score_proteique", False),
        "Note Sante": ("score_sante", False),
        "Prix/kg (croissant)": ("prix_par_kg", True),
        "Proteines/100g (decroissant)": ("proteines_100g", False),
    }
    sort_col, sort_asc = sort_map.get(sort_option, ("score_final", False))

    if sort_col in filtered_df.columns:
        sorted_df = filtered_df.sort_values(sort_col, ascending=sort_asc, na_position="last")
    else:
        sorted_df = filtered_df

    st.markdown(f"**{len(sorted_df)} produits affiches** sur {len(df)}")

    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        confidence = row.get("offer_confidence")
        render_product_card_v2(rank, row)
        badge_html = get_confidence_badge(confidence)
        st.html(CARD_CSS + f"<div style='margin-top:-8px;margin-bottom:12px;padding-left:95px;'>{badge_html}</div>")

    st.divider()
    st.subheader("📊 Tableau complet")

    display_cols = [
        "nom", "marque", "proteines_100g", "bcaa_per_100g_prot", "leucine_g",
        "prix_par_kg", "prix", "poids_kg",
        "type_whey", "origin_label", "ingredient_count",
        "has_sucralose", "has_acesulfame_k", "has_aspartame",
        "has_artificial_flavors", "has_thickeners", "has_colorants",
        "profil_suspect",
        "score_proteique", "score_sante", "score_final",
    ]
    existing_cols = [c for c in display_cols if c in sorted_df.columns]

    column_config = {
        "nom": st.column_config.TextColumn("Produit", width="large"),
        "marque": st.column_config.TextColumn("Marque"),
        "proteines_100g": st.column_config.NumberColumn("Prot/100g", format="%.1f g"),
        "bcaa_per_100g_prot": st.column_config.NumberColumn("BCAA/100g prot", format="%.1f g"),
        "leucine_g": st.column_config.NumberColumn("Leucine/100g prot", format="%.1f g"),
        "prix_par_kg": st.column_config.NumberColumn("Prix/kg", format="%.0f €"),
        "prix": st.column_config.NumberColumn("Prix", format="%.2f €"),
        "poids_kg": st.column_config.NumberColumn("Poids", format="%.2f kg"),
        "type_whey": st.column_config.TextColumn("Type"),
        "origin_label": st.column_config.TextColumn("Origine"),
        "ingredient_count": st.column_config.NumberColumn("Nb ingr."),
        "has_sucralose": st.column_config.CheckboxColumn("Sucralose"),
        "has_acesulfame_k": st.column_config.CheckboxColumn("Ace-K"),
        "has_aspartame": st.column_config.CheckboxColumn("Aspartame"),
        "has_artificial_flavors": st.column_config.CheckboxColumn("Aromes art."),
        "has_thickeners": st.column_config.CheckboxColumn("Epaississants"),
        "has_colorants": st.column_config.CheckboxColumn("Colorants"),
        "profil_suspect": st.column_config.CheckboxColumn("Profil suspect"),
        "score_proteique": st.column_config.ProgressColumn("Proteique /10", min_value=0, max_value=10),
        "score_sante": st.column_config.ProgressColumn("Sante /10", min_value=0, max_value=10),
        "score_final": st.column_config.ProgressColumn("Finale /10", min_value=0, max_value=10),
    }

    st.dataframe(
        sorted_df[existing_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("📥 Exporter")

    col_e1, col_e2 = st.columns(2)
    csv_data = sorted_df.to_csv(index=False, sep=";", encoding="utf-8-sig")

    with col_e1:
        st.download_button(
            label="Telecharger CSV",
            data=csv_data,
            file_name=f"catalogue_whey_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
            key="cat_csv_download",
        )

    with col_e2:
        excel_path = "/tmp/catalogue_export.xlsx"
        sorted_df.to_excel(excel_path, index=False, sheet_name="Catalogue")
        with open(excel_path, "rb") as f:
            st.download_button(
                label="Telecharger Excel",
                data=f.read(),
                file_name=f"catalogue_whey_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="cat_excel_download",
            )


# ── SIDEBAR ──

def render_sidebar():
    user = st.session_state.user
    current_page = st.session_state.page
    with st.sidebar:
        if LOGO_B64:
            st.markdown(f"""
            <div class='sidebar-logo'>
                <img src='data:image/png;base64,{LOGO_B64}' style='height:44px;width:auto;mix-blend-mode:lighten;' />
            </div>
            """, unsafe_allow_html=True)
        else:
            st.html("""
            <div class='sidebar-logo'>
                <span class='sidebar-logo-icon'>🧪</span>
                <span class='sidebar-logo-text'>ProteinScan</span>
            </div>
            """)

        plan_label = "Pro" if user["plan"] == "pro" else "Gratuit"
        plan_icon = "⭐" if user["plan"] == "pro" else "📋"
        st.html(f"""
        <div class='sidebar-user'>
            <div class='sidebar-user-name'>👤 {html_module.escape(user['display_name'])}</div>
            <div class='sidebar-user-email'>{html_module.escape(user['email'])}</div>
            <div class='sidebar-user-plan'>{plan_icon} Plan: {plan_label}</div>
        </div>
        """)

        if st.button("📦 Catalogue", use_container_width=True, type="primary" if current_page == "catalogue" else "secondary"):
            st.session_state.page = "catalogue"
            st.session_state.view_scan_id = None
            st.rerun()

        if st.button("🧪 Nouveau scan", type="primary" if current_page == "scan" else "secondary", use_container_width=True):
            st.session_state.page = "scan"
            st.session_state.view_scan_id = None
            st.rerun()

        if st.button("📊 Tableau de bord", use_container_width=True, type="primary" if current_page == "dashboard" else "secondary"):
            st.session_state.page = "dashboard"
            st.session_state.view_scan_id = None
            st.rerun()

        if st.button("⚙️ Admin", use_container_width=True, type="primary" if current_page == "admin" else "secondary"):
            st.session_state.page = "admin"
            st.session_state.view_scan_id = None
            st.rerun()

        st.markdown("---")
        if st.button("🚪 Se deconnecter", use_container_width=True):
            logout()
            st.rerun()


# ── AUTH PAGES ──

def page_login():
    if LOGO_B64:
        st.markdown(f"""
        <div style='text-align:center;padding:40px 0 10px 0;'>
            <img src='data:image/png;base64,{LOGO_B64}' style='height:160px;width:auto;mix-blend-mode:lighten;' />
        </div>
        <div style='text-align:center;color:#6b85b0;font-size:1em;margin-bottom:36px;letter-spacing:0.3px;'>
            Comparateur de proteines whey en France
        </div>
        """, unsafe_allow_html=True)
    else:
        st.html("""
        <div class='login-header'>
            <div class='login-logo'>🧪</div>
            <div class='login-title'>ProteinScan</div>
            <div class='login-subtitle'>Comparateur de proteines whey en France</div>
        </div>
        """)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Connexion")
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Mot de passe", type="password")
            submitted = st.form_submit_button("Se connecter", use_container_width=True)

            if submitted:
                if not email or not password:
                    st.error("Veuillez remplir tous les champs.")
                else:
                    user = get_user_by_email(email)
                    if user and verify_password(password, user["password_hash"]):
                        st.session_state.user = {
                            "id": user["id"],
                            "email": user["email"],
                            "display_name": user["display_name"],
                            "plan": user["plan"],
                        }
                        st.session_state.page = "dashboard"
                        st.session_state.view_scan_id = None
                        st.rerun()
                    else:
                        st.error("Email ou mot de passe incorrect.")

    with col_right:
        st.subheader("Inscription")
        with st.form("signup_form"):
            new_name = st.text_input("Nom")
            new_email = st.text_input("Email", key="signup_email")
            new_password = st.text_input("Mot de passe", type="password", key="signup_password")
            new_password2 = st.text_input("Confirmer le mot de passe", type="password", key="signup_password2")
            signup_submitted = st.form_submit_button("Creer mon compte", use_container_width=True)

            if signup_submitted:
                if not new_name or not new_email or not new_password:
                    st.error("Veuillez remplir tous les champs.")
                elif len(new_password) < 6:
                    st.error("Le mot de passe doit contenir au moins 6 caracteres.")
                elif new_password != new_password2:
                    st.error("Les mots de passe ne correspondent pas.")
                elif "@" not in new_email:
                    st.error("Veuillez entrer une adresse email valide.")
                else:
                    pw_hash = hash_password(new_password)
                    user = create_user(new_email, pw_hash, new_name)
                    if user:
                        st.session_state.user = {
                            "id": user["id"],
                            "email": user["email"],
                            "display_name": user["display_name"],
                            "plan": user["plan"],
                        }
                        st.session_state.page = "dashboard"
                        st.rerun()
                    else:
                        st.error("Cet email est deja utilise.")

    st.html("""
    <div style='text-align:center;margin-top:32px;padding:16px 24px;background:linear-gradient(135deg, rgba(12,17,30,0.6), rgba(10,14,24,0.4));border:1px solid rgba(74,158,237,0.12);border-radius:12px;color:#6b85b0;font-size:0.88em;'>
        <span style='color:#4a9eed;font-weight:700;'>Plan Gratuit</span> : 3 scans par mois &nbsp;|&nbsp;
        <span style='color:#4a9eed;font-weight:700;'>Plan Pro</span> : scans illimites (bientot disponible)
    </div>
    """)


# ── DASHBOARD ──

def page_dashboard():
    user = st.session_state.user
    render_sidebar()

    if st.session_state.view_scan_id:
        page_view_scan()
        return

    render_page_header("Tableau de bord")
    st.html(f"<div class='page-subtitle'>Bienvenue, {html_module.escape(user['display_name'])}</div>")

    scans_history = get_user_scans(user["id"])
    plan_label = "Pro" if user["plan"] == "pro" else "Gratuit"

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Plan", plan_label)
    with col2:
        st.metric("Total scans", len(scans_history))
    with col3:
        if user["plan"] == "free":
            st.html("<div style='padding-top:12px;'><span class='upgrade-btn'>Mettre a niveau</span></div>")
        else:
            st.metric("Statut", "Actif")

    st.markdown("---")
    st.html("<div class='section-title'>📋 Historique des scans</div>")

    if not scans_history:
        st.info("Vous n'avez pas encore effectue de scan. Cliquez sur **Nouveau scan** pour commencer !")
    else:
        for scan in scans_history:
            created = scan["created_at"]
            if isinstance(created, str):
                date_str = created
            else:
                date_str = created.strftime("%d/%m/%Y a %H:%M")

            col_date, col_count, col_action = st.columns([3, 2, 2])
            with col_date:
                st.html(f"<div class='scan-row-date'>📅 {html_module.escape(date_str)}</div>")
            with col_count:
                st.html(f"<div class='scan-row-count'>📦 {scan['product_count']} produits</div>")
            with col_action:
                if st.button("Voir les resultats", key=f"view_{scan['id']}", use_container_width=True):
                    st.session_state.view_scan_id = scan["id"]
                    st.rerun()
            st.markdown("---")


def page_view_scan():
    scan_id = st.session_state.view_scan_id
    user = st.session_state.user
    items = get_scan_items(scan_id, user["id"])

    if st.button("← Retour au tableau de bord"):
        st.session_state.view_scan_id = None
        st.rerun()

    if not items:
        st.warning("Aucun produit dans ce scan.")
        return

    st.title(f"🏆 Classement — Scan #{scan_id}")
    render_results(items, is_dataframe=False)


# ── SCAN PAGE ──

def page_scan():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Nouveau scan")

    tab_auto, tab_manual = st.tabs(["Scan automatique", "Analyse manuelle d'URLs"])

    with tab_auto:
        if not api_key:
            st.warning("La cle API Brave Search n'est pas configuree sur le serveur.")
        else:
            st.markdown(f"Lance une recherche sur **{len(SEARCH_QUERIES)} requetes** pour trouver les meilleurs produits whey du marche francais.")
            scan_button = st.button("🚀 Lancer le scan", type="primary", use_container_width=False)

            if scan_button:
                st.session_state.last_scan_results = None
                status_container = st.empty()
                progress_bar = st.progress(0)
                detail_text = st.empty()

                def update_progress(current, total, detail=""):
                    if total > 0:
                        pct = min(current / total, 1.0)
                        progress_bar.progress(pct)
                    detail_text.text(detail)

                def update_status(msg):
                    status_container.info(msg)

                try:
                    with st.spinner("Analyse en cours..."):
                        products = scrape_products(
                            api_key=api_key,
                            progress_callback=update_progress,
                            status_callback=update_status,
                        )

                    progress_bar.empty()
                    detail_text.empty()

                    if products:
                        scan_id = save_scan(user["id"], products)
                        status_container.success(
                            f"✅ {len(products)} produits trouves et sauvegardes ! (Scan #{scan_id})"
                        )
                        st.session_state.last_scan_results = products
                        st.session_state.last_scan_id = scan_id
                    else:
                        status_container.warning("Aucun produit trouve.")

                except BraveAPIError as e:
                    progress_bar.empty()
                    detail_text.empty()
                    status_container.error(f"Erreur API : {e}")

            if "last_scan_results" in st.session_state and st.session_state.last_scan_results:
                df = pd.DataFrame(st.session_state.last_scan_results)
                st.divider()
                render_results(df)

    with tab_manual:
        st.markdown("Entrez des URLs de pages produit (une par ligne) :")
        urls_input = st.text_area(
            "URLs de pages produit",
            placeholder="https://www.myprotein.fr/...\nhttps://www.bulk.com/fr/...",
            height=150,
        )

        analyze_button = st.button("🔍 Analyser ces URLs", type="primary", key="manual_analyze")

        if analyze_button and urls_input.strip():
            urls = [u.strip() for u in urls_input.strip().split("\n") if u.strip().startswith("http")]

            if not urls:
                st.warning("Aucune URL valide trouvee.")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()

                from concurrent.futures import ThreadPoolExecutor, as_completed
                products = []
                completed_count = 0
                total_count = len(urls)

                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = {executor.submit(extract_product_data, url): url for url in urls}
                    for future in as_completed(futures):
                        completed_count += 1
                        url = futures[future]
                        status_text.text(f"Extraction {completed_count}/{total_count} : {url[:80]}...")
                        progress_bar.progress(completed_count / total_count)
                        try:
                            result = future.result()
                            if result:
                                products.append(result)
                        except Exception:
                            pass

                progress_bar.empty()
                status_text.empty()

                if products:
                    scan_id = save_scan(user["id"], products)
                    st.success(
                        f"✅ {len(products)} produits extraits et sauvegardes ! (Scan #{scan_id})"
                    )
                    st.session_state.last_manual_results = products
                    st.session_state.last_manual_scan_id = scan_id
                else:
                    st.warning("Aucune donnee produit extraite.")

        if "last_manual_results" in st.session_state and st.session_state.last_manual_results:
            df = pd.DataFrame(st.session_state.last_manual_results)
            st.divider()
            render_results(df)


# ── CATALOGUE PAGE ──

def page_catalogue():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Catalogue de produits")

    stats = get_catalog_stats()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Produits", stats["total_products"])
    with col2:
        st.metric("Offres actives", stats["total_active_offers"])
    with col3:
        st.metric("Confiance moy.", f"{stats['avg_confidence']:.0%}")
    with col4:
        last_disc = stats["last_discovery"]
        if last_disc:
            if isinstance(last_disc, str):
                disc_display = last_disc
            else:
                disc_display = last_disc.strftime("%d/%m/%Y %H:%M")
        else:
            disc_display = "Jamais"
        st.metric("Dernier discovery", disc_display)
    with col5:
        st.metric("A revoir", stats["products_needing_review"])

    st.divider()

    min_confidence = st.slider(
        "Confiance minimum des offres",
        min_value=0.0,
        max_value=1.0,
        value=0.75,
        step=0.05,
        key="cat_confidence_slider",
        help="Les produits avec confidence < 0.75 sont masques par defaut (ex: pages JS sans prix).",
    )

    products = get_all_products(min_confidence=min_confidence, limit=200)

    if not products:
        st.info("Aucun produit dans le catalogue. Lancez un Discovery depuis la page Admin pour alimenter le catalogue.")
        return

    render_catalog_results(products)


# ── ADMIN PAGE ──

def page_admin():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Administration")

    stats = get_catalog_stats()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Produits", stats["total_products"])
    with col2:
        st.metric("Offres actives", stats["total_active_offers"])
    with col3:
        st.metric("Confiance moy.", f"{stats['avg_confidence']:.0%}")
    with col4:
        st.metric("A revoir", stats["products_needing_review"])

    st.divider()

    disc_col, refresh_col = st.columns(2)

    with disc_col:
        st.subheader("🔍 Discovery")
        st.markdown("Recherche de nouveaux produits whey via Brave Search avec diversification par marque et domaine.")

        if not api_key:
            st.warning("Cle API Brave Search non configuree.")
        else:
            with st.expander("Parametres Discovery", expanded=False):
                disc_max_per_domain = st.number_input(
                    "Max URLs par domaine", min_value=1, max_value=10,
                    value=MAX_PER_DOMAIN, key="disc_max_domain"
                )
                disc_use_seeds = st.checkbox("Utiliser les marques seed", value=True, key="disc_seeds")
                disc_scrape_limit = st.number_input(
                    "Limite URLs a scraper", min_value=10, max_value=500,
                    value=200, key="disc_limit"
                )
                disc_block_str = st.text_input(
                    "Domaines bloques (separes par virgule)",
                    value=", ".join(BLOCK_DOMAINS),
                    key="disc_block"
                )
                disc_whey_filter = st.checkbox("Filtre whey strict (2+ signaux)", value=True, key="disc_whey_filter")
                disc_use_resolver = st.checkbox("Resolveur d'URL (crawl 1 niveau)", value=True, key="disc_resolver")

            if st.button("🚀 Lancer un Discovery", type="primary", use_container_width=True, key="btn_discovery"):
                status_container = st.empty()
                progress_bar = st.progress(0)
                detail_text = st.empty()

                def disc_progress(current, total, detail=""):
                    if total > 0:
                        progress_bar.progress(min(current / total, 1.0))
                    detail_text.text(detail)

                def disc_status(msg):
                    status_container.info(msg)

                block_list = [d.strip() for d in disc_block_str.split(",") if d.strip()]

                try:
                    with st.spinner("Discovery en cours..."):
                        result = run_discovery(
                            api_key=api_key,
                            progress_callback=disc_progress,
                            status_callback=disc_status,
                            max_per_domain=disc_max_per_domain,
                            use_brand_seeds=disc_use_seeds,
                            block_domains=block_list,
                            scrape_limit=disc_scrape_limit,
                            use_whey_filter=disc_whey_filter,
                            use_resolver=disc_use_resolver,
                        )
                    progress_bar.empty()
                    detail_text.empty()

                    domains_found = result.get("domains_found", [])
                    brands_missing = result.get("brands_missing", [])
                    msg = (
                        f"Produits: {result.get('products_found', 0)}, "
                        f"Offres: {result.get('offers_created', 0)}, "
                        f"Domaines: {len(domains_found)}, "
                        f"Erreurs: {result.get('errors', 0)}"
                    )
                    if result.get("whey_rejected"):
                        msg += f"\nNon-whey rejetes: {result['whey_rejected']}"
                    if result.get("resolved"):
                        msg += f"\nURLs resolues: {result['resolved']}"
                    if brands_missing:
                        msg += f"\nMarques manquantes: {', '.join(sorted(brands_missing)[:10])}"
                    status_container.success(f"✅ Discovery termine !\n{msg}")
                except Exception as e:
                    progress_bar.empty()
                    detail_text.empty()
                    status_container.error(f"Erreur Discovery : {e}")

    with refresh_col:
        st.subheader("🔄 Refresh")
        st.markdown("Met a jour les prix et la disponibilite des offres existantes.")

        if st.button("🔄 Lancer un Refresh", type="primary", use_container_width=True, key="btn_refresh"):
            status_container_r = st.empty()
            progress_bar_r = st.progress(0)
            detail_text_r = st.empty()

            def ref_progress(current, total, detail=""):
                if total > 0:
                    progress_bar_r.progress(min(current / total, 1.0))
                detail_text_r.text(detail)

            def ref_status(msg):
                status_container_r.info(msg)

            try:
                with st.spinner("Refresh en cours..."):
                    result = run_refresh(
                        progress_callback=ref_progress,
                        status_callback=ref_status,
                    )
                progress_bar_r.empty()
                detail_text_r.empty()
                status_container_r.success(
                    f"✅ Refresh termine ! "
                    f"Offres mises a jour: {result.get('offers_updated', 0)}, "
                    f"Erreurs: {result.get('errors', 0)}"
                )
            except Exception as e:
                progress_bar_r.empty()
                detail_text_r.empty()
                status_container_r.error(f"Erreur Refresh : {e}")

    st.divider()
    st.subheader("📋 Historique des pipelines")

    runs = get_pipeline_runs(limit=20)
    if not runs:
        st.info("Aucun pipeline execute pour le moment.")
    else:
        runs_data = []
        for r in runs:
            started = r["started_at"]
            if isinstance(started, datetime):
                started = started.strftime("%d/%m/%Y %H:%M")
            finished = r.get("finished_at")
            if isinstance(finished, datetime):
                finished = finished.strftime("%d/%m/%Y %H:%M")
            elif finished is None:
                finished = "—"

            runs_data.append({
                "Date": started,
                "Type": r["run_type"].capitalize(),
                "Statut": r["status"].capitalize(),
                "Produits": r.get("products_found", 0),
                "Offres MAJ": r.get("offers_updated", 0),
                "Erreurs": r.get("errors", 0),
                "Fin": finished,
            })

        st.dataframe(
            pd.DataFrame(runs_data),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.subheader("🏥 Discovery Health")
    st.markdown("Vue d'ensemble de la couverture du catalogue : domaines, marques detectees, marques manquantes.")

    try:
        disc_stats = get_discovery_stats_from_db()

        h1, h2, h3 = st.columns(3)
        with h1:
            st.metric("Domaines uniques", disc_stats["unique_domains"])
        with h2:
            st.metric("Marques seed trouvees", f"{len(disc_stats['brands_in_catalog'])}/{disc_stats['total_seed_brands']}")
        with h3:
            st.metric("Marques manquantes", len(disc_stats["brands_missing"]))

        if disc_stats["brands_missing"]:
            st.warning(f"Marques seed non trouvees dans le catalogue : **{', '.join(disc_stats['brands_missing'])}**")

        if disc_stats["brands_in_catalog"]:
            st.success(f"Marques seed presentes : {', '.join(disc_stats['brands_in_catalog'])}")

        col_dom, col_brands = st.columns(2)

        with col_dom:
            st.markdown("**Top domaines (offres actives)**")
            if disc_stats["domain_stats"]:
                dom_data = []
                for d in disc_stats["domain_stats"]:
                    avg_conf = d.get("avg_confidence")
                    dom_data.append({
                        "Domaine": d["merchant"],
                        "Offres": d["offer_count"],
                        "Produits": d["product_count"],
                        "Confiance moy.": f"{avg_conf:.0%}" if avg_conf else "—",
                    })
                st.dataframe(pd.DataFrame(dom_data), use_container_width=True, hide_index=True)
            else:
                st.info("Aucune offre active.")

        with col_brands:
            st.markdown("**Marques dans le catalogue**")
            if disc_stats["brand_details"]:
                brand_data = [{
                    "Marque": b["brand"].title(),
                    "Produits": b["product_count"],
                } for b in disc_stats["brand_details"]]
                st.dataframe(pd.DataFrame(brand_data), use_container_width=True, hide_index=True)
            else:
                st.info("Aucune marque detectee.")

    except Exception as e:
        st.error(f"Erreur chargement des stats Discovery Health : {e}")

    st.divider()
    st.subheader("🔎 Debug : Validateur de page produit")
    st.markdown("Testez si une URL sera acceptee ou rejetee par le validateur strict.")

    debug_url = st.text_input("URL a tester", placeholder="https://example.fr/produit/whey-isolate-1kg", key="debug_url")

    if st.button("Tester cette URL", key="btn_debug_validate"):
        if not debug_url or not debug_url.startswith("http"):
            st.warning("Entrez une URL valide (commencant par http).")
        else:
            with st.spinner("Analyse de la page en cours..."):
                debug_result = validate_url_debug(debug_url)

            page_type = debug_result.get("page_type", "unknown")
            type_colors = {
                "product": "green",
                "article": "red",
                "category": "orange",
                "blocked": "red",
                "unknown": "gray",
            }
            type_color = type_colors.get(page_type, "gray")
            st.markdown(f"**Type de page :** :{type_color}[{page_type.upper()}]")

            if debug_result["status"] == "accepted":
                st.success(f"Page ACCEPTEE via : {debug_result['reasons'].get('acceptance_path', '?')}")
            elif debug_result["status"] == "rejected_url":
                st.error(f"URL REJETEE (pre-filtre) : {debug_result['bad_url_reason']}")
            elif debug_result["status"] == "rejected":
                st.error(f"Page REJETEE : {debug_result['reasons'].get('rejection_reason', '?')}")
            elif debug_result.get("error"):
                st.error(f"Erreur : {debug_result['error']}")
            else:
                st.warning(f"Statut : {debug_result['status']}")

            col_proof, col_article = st.columns(2)
            with col_proof:
                has_proof = debug_result.get("has_purchase_proof", False)
                proof_details = debug_result.get("purchase_proof", [])
                if has_proof:
                    st.success(f"Preuve d'achat : OUI")
                    for p in proof_details:
                        st.caption(f"  - {p}")
                else:
                    st.error("Preuve d'achat : NON")
                    st.caption("Pas de JSON-LD Offer+price, pas de meta prix, pas de bouton panier+prix")

            with col_article:
                is_article = debug_result.get("is_article", False)
                article_signals = debug_result.get("article_signals", [])
                if is_article:
                    st.error(f"Page article/guide detectee")
                    for s in article_signals:
                        st.caption(f"  - {s}")
                else:
                    st.success("Page non-article")

            with st.expander("Signaux detailles", expanded=True):
                signals = debug_result.get("reasons", {}).get("signals", {})

                if signals.get("jsonld"):
                    jl = signals["jsonld"]
                    cols = st.columns(4)
                    cols[0].metric("JSON-LD Product", "Oui" if jl.get("has_product") else "Non")
                    cols[1].metric("Offer", "Oui" if jl.get("has_offer") else "Non")
                    cols[2].metric("Prix", "Oui" if jl.get("has_price") else "Non")
                    cols[3].metric("Disponibilite", "Oui" if jl.get("has_availability") else "Non")
                    if jl.get("product_name"):
                        st.text(f"Nom produit JSON-LD : {jl['product_name'][:100]}")

                signal_labels = {
                    "add_to_cart": "Signaux panier",
                    "price": "Signaux prix",
                    "weight": "Signaux poids",
                }
                for key, label in signal_labels.items():
                    sigs = signals.get(key, [])
                    if sigs:
                        st.success(f"{label} : {', '.join(str(s) for s in sigs)}")
                    else:
                        st.warning(f"{label} : aucun signal detecte")

            with st.expander("Donnees brutes JSON"):
                st.json(debug_result)

    st.divider()
    st.subheader("🔗 Debug : Resolveur d'URL whey")
    st.markdown("Testez la resolution de liens : si l'URL n'est pas une page produit whey, le resolveur cherche le meilleur lien produit sur la page.")

    resolver_url = st.text_input("URL a resoudre", placeholder="https://example.fr/marque/whey", key="resolver_url")

    if st.button("Resoudre cette URL", key="btn_debug_resolve"):
        if not resolver_url or not resolver_url.startswith("http"):
            st.warning("Entrez une URL valide (commencant par http).")
        else:
            with st.spinner("Resolution en cours (peut prendre 30s)..."):
                resolve_result = resolve_url_debug(resolver_url)

            if resolve_result.get("resolved_url"):
                if resolve_result.get("is_start_whey_product"):
                    st.success(f"L'URL est deja une page produit whey valide.")
                else:
                    st.success(f"Produit whey trouve : {resolve_result['resolved_url']}")
                    st.info(f"Methode : {resolve_result.get('resolution_method', '?')}")
            else:
                st.error("Aucun produit whey trouve via cette URL.")

            with st.expander("Etapes de resolution", expanded=True):
                for reason in resolve_result.get("reasons", []):
                    st.text(f"- {reason}")

            whey_detail = resolve_result.get("start_whey_detail")
            if whey_detail:
                with st.expander("Signaux whey de la page initiale"):
                    cols = st.columns(3)
                    cols[0].metric("Page produit", "Oui" if whey_detail.get("is_product") else "Non")
                    cols[1].metric("Est whey", "Oui" if whey_detail.get("is_whey") else "Non")
                    cols[2].metric("Signaux whey", str(whey_detail.get("whey_signal_count", 0)))
                    if whey_detail.get("whey_signals"):
                        st.write("Signaux detectes :")
                        for sig in whey_detail["whey_signals"]:
                            st.text(f"  - {sig}")
                    if whey_detail.get("non_whey_signals"):
                        st.warning(f"Signaux non-whey : {', '.join(whey_detail['non_whey_signals'])}")

            candidates = resolve_result.get("candidates_top10", [])
            if candidates:
                with st.expander(f"Top {len(candidates)} candidats testes"):
                    for i, c in enumerate(candidates):
                        is_whey = c.get("is_whey_product", False)
                        icon = "✅" if is_whey else "❌"
                        st.text(f"{icon} [{c.get('score', 0):+d}] {c['url'][:80]}")
                        if c.get("anchor"):
                            st.text(f"   Texte lien : {c['anchor'][:60]}")
                        if c.get("whey_signals"):
                            st.text(f"   Signaux : {', '.join(c['whey_signals'])}")
                        if c.get("test_result"):
                            st.text(f"   Resultat : {c['test_result']}")

            with st.expander("JSON complet"):
                st.json(resolve_result)


# ── ROUTER ──

if st.session_state.user is None:
    page_login()
else:
    page = st.session_state.page
    if page == "dashboard":
        page_dashboard()
    elif page == "scan":
        page_scan()
    elif page == "catalogue":
        page_catalogue()
    elif page == "admin":
        page_admin()
    elif page == "login":
        st.session_state.page = "dashboard"
        page_dashboard()
    else:
        page_dashboard()
