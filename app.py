import streamlit as st
import pandas as pd
import os
import base64
import html as html_module
from datetime import datetime

from scraper import (
    extract_product_data,
    run_discovery, run_refresh, get_discovery_stats_from_db,
    SEED_BRANDS, BLOCK_DOMAINS, MAX_PER_DOMAIN,
)
from scoring import calculate_price_score, calculate_final_score_10, calculate_price_score_10
from db import (
    init_db, create_user, get_user_by_email,
    get_all_products, get_catalog_stats, get_pipeline_runs,
    get_product_by_id, get_product_offers, get_products_by_ids,
    create_review, get_reviews_for_product, get_average_rating,
    flag_review, hide_review, get_flagged_reviews,
    get_data_quality_stats, cleanup_catalog, get_incomplete_products_for_rescrape,
    create_recommendation, get_recommendations_for_product, get_top_products,
)
from auth import hash_password, verify_password
from page_validator import validate_url_debug, is_whey_product_page
from resolver import resolve_url_debug

init_db()


@st.cache_data(ttl=300)
def cached_get_all_products(min_confidence=0.0, limit=300):
    return get_all_products(min_confidence=min_confidence, limit=limit)


@st.cache_data(ttl=120)
def cached_get_product_by_id(product_id):
    return get_product_by_id(product_id)


@st.cache_data(ttl=120)
def cached_get_product_offers(product_id):
    return get_product_offers(product_id)


@st.cache_data(ttl=30)
def cached_get_reviews(product_id):
    return get_reviews_for_product(product_id)


@st.cache_data(ttl=30)
def cached_get_average_rating(product_id):
    return get_average_rating(product_id)


@st.cache_data(ttl=300)
def cached_get_catalog_stats():
    return get_catalog_stats()


@st.cache_data(ttl=30)
def cached_get_products_by_ids(product_ids):
    return get_products_by_ids(product_ids)


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
    background: #080a14 !important;
}

.stApp > header,
[data-testid="stHeader"] {
    background: transparent !important;
}

[data-testid="stHeader"] [data-testid="stDecoration"],
[data-testid="stHeader"] [data-testid="stToolbar"] {
    background: transparent !important;
}

[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main,
.block-container,
[data-testid="stAppViewBlockContainer"],
[data-testid="stVerticalBlock"],
.stMainBlockContainer {
    background: transparent !important;
}

.stMainBlockContainer {
    border-radius: 0 !important;
    margin: 0 !important;
    padding: 24px 32px !important;
    border: none !important;
}

@media (max-width: 768px) {
    .stMainBlockContainer { padding: 12px 8px !important; }
}

section[data-testid="stSidebar"] {
    background: #0c0e1e !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
    padding-top: 8px !important;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"],
section[data-testid="stSidebar"] ul[data-testid="stSidebarNavItems"],
section[data-testid="stSidebar"] .st-emotion-cache-eczf16,
section[data-testid="stSidebar"] nav,
[data-testid="stSidebarNavLink"],
[data-testid="stSidebarNavSeparator"] {
    display: none !important;
}

section[data-testid="stSidebar"] [data-testid="stPageLink-NavLink"] {
    display: none !important;
}

[data-testid="stHeader"] [data-testid="stPageLink-NavLink"],
[data-testid="stHeader"] .st-emotion-cache-eczf16 {
    display: none !important;
}

section[data-testid="stSidebar"] .stMarkdown h1,
section[data-testid="stSidebar"] .stMarkdown h2,
section[data-testid="stSidebar"] .stMarkdown h3 {
    color: #f0f2f5 !important;
}

div[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 8px !important;
    padding: 18px 22px !important;
}

div[data-testid="stMetric"]:hover {
    border-color: rgba(255,255,255,0.1) !important;
}

div[data-testid="stMetric"] label {
    color: #8b95a5 !important;
    font-size: 0.78em !important;
    text-transform: uppercase !important;
    letter-spacing: 0.6px !important;
    font-weight: 500 !important;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f0f2f5 !important;
    font-weight: 700 !important;
}

.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: 0.88em !important;
    transition: all 0.15s ease !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    background: rgba(255,255,255,0.04) !important;
    color: #c0c8d4 !important;
    padding: 8px 20px !important;
}

.stButton > button:hover {
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(255,255,255,0.16) !important;
    transform: none !important;
}

.stButton > button:active {
    transform: none !important;
}

.stButton > button[kind="primary"] {
    background: #2563eb !important;
    color: white !important;
    border: 1px solid #2563eb !important;
}

.stButton > button[kind="primary"]:hover {
    background: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
    transform: none !important;
}

div[data-testid="stForm"] {
    background: rgba(255,255,255,0.02) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 8px !important;
    padding: 28px 24px !important;
}

.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div {
    background: rgba(0,0,0,0.3) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 6px !important;
    color: #f0f2f5 !important;
    padding: 10px 14px !important;
}

.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #2563eb !important;
    box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.15) !important;
}

.stTextInput label,
.stTextArea label,
.stSelectbox label {
    color: #8b95a5 !important;
    font-size: 0.85em !important;
    font-weight: 500 !important;
}

div[data-testid="stExpander"] {
    background: rgba(255,255,255,0.02) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 8px !important;
}

div[data-testid="stExpander"]:hover {
    border-color: rgba(255,255,255,0.1) !important;
}

.stDataFrame {
    border-radius: 8px !important;
    overflow: hidden !important;
}

.stTabs [data-baseweb="tab-list"] {
    gap: 4px !important;
    background: rgba(255,255,255,0.03) !important;
    border-radius: 8px !important;
    padding: 4px !important;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 6px !important;
    color: #8b95a5 !important;
    font-weight: 500 !important;
    padding: 8px 18px !important;
}

.stTabs [aria-selected="true"] {
    background: rgba(37, 99, 235, 0.15) !important;
    color: #6ba1eb !important;
}

hr {
    border-color: rgba(255,255,255,0.06) !important;
    margin: 24px 0 !important;
}

.stProgress > div > div > div {
    background: #2563eb !important;
    border-radius: 4px !important;
}

div[data-testid="stSlider"] > div > div {
    color: #6ba1eb !important;
}

.stDownloadButton > button {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    color: #6ba1eb !important;
    border-radius: 8px !important;
}

.stDownloadButton > button:hover {
    background: rgba(255,255,255,0.08) !important;
}

div.stAlert {
    border-radius: 8px !important;
}

.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 0 16px 0;
    margin-bottom: 10px;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}

.sidebar-logo-icon {
    font-size: 1.5em;
    color: #2563eb;
}

.sidebar-logo-text {
    font-size: 1.2em;
    font-weight: 700;
    color: #f0f2f5;
    letter-spacing: -0.3px;
}

.sidebar-user {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 14px 16px;
    margin: 12px 0 16px 0;
}

.sidebar-user-name {
    font-weight: 600;
    font-size: 0.95em;
    color: #f0f2f5;
}

.sidebar-user-email {
    font-size: 0.8em;
    color: #8b95a5;
    margin-top: 2px;
}

.sidebar-user-plan {
    font-size: 0.75em;
    color: #6ba1eb;
    font-weight: 600;
    margin-top: 6px;
}

.page-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
    padding-bottom: 8px;
}

.page-header-icon {
    font-size: 1.4em;
    color: #2563eb;
}

.page-header-title {
    font-size: 1.6em;
    font-weight: 700;
    color: #f0f2f5;
    letter-spacing: -0.3px;
}

.page-subtitle {
    color: #8b95a5;
    font-size: 0.9em;
    margin-bottom: 24px;
}

.stat-card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 20px 24px;
    margin-bottom: 16px;
}

.stat-card:hover {
    border-color: rgba(255,255,255,0.1);
}

.stat-card-label {
    font-size: 0.72em;
    color: #8b95a5;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 4px;
}

.stat-card-value {
    font-size: 1.6em;
    font-weight: 700;
    color: #f0f2f5;
}

.scan-row {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
}

.scan-row:hover {
    border-color: rgba(255,255,255,0.1);
}

.scan-row-date {
    color: #f0f2f5;
    font-size: 0.88em;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 8px;
}

.scan-row-count {
    color: #f59e0b;
    font-size: 0.88em;
    font-weight: 600;
}

.login-container {
    max-width: 900px;
    margin: 0 auto;
    padding-top: 48px;
}

.login-header {
    text-align: center;
    margin-bottom: 48px;
}

.login-logo {
    font-size: 2.5em;
    margin-bottom: 8px;
    color: #2563eb;
}

.login-title {
    font-size: 1.8em;
    font-weight: 700;
    color: #f0f2f5;
    margin-bottom: 6px;
}

.login-subtitle {
    font-size: 0.95em;
    color: #8b95a5;
}

.login-plans {
    text-align: center;
    margin-top: 32px;
    padding: 16px 24px;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    color: #8b95a5;
    font-size: 0.88em;
}

.ps-card {
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 20px 22px;
    margin-bottom: 12px;
    background: rgba(255,255,255,0.02);
    transition: border-color 0.15s ease;
}
.ps-card:hover {
    border-color: rgba(255,255,255,0.12);
}
.ps-rank {
    font-size: 1.5em;
    font-weight: 700;
    text-align: center;
    line-height: 1.1;
}
.ps-stars {
    color: #f59e0b;
    font-size: 1.2em;
    letter-spacing: 1px;
}
.ps-stars-sm {
    color: #f59e0b;
    font-size: 0.9em;
}
.ps-score-num {
    font-size: 0.82em;
    color: #8b95a5;
    margin-left: 4px;
}
.ps-title {
    font-size: 1.1em;
    font-weight: 700;
    margin-bottom: 3px;
    line-height: 1.3;
    color: #f0f2f5;
}
.ps-brand {
    font-size: 0.85em;
    color: #8b95a5;
}
.ps-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 4px;
    font-size: 0.75em;
    font-weight: 500;
    margin: 2px 3px;
    line-height: 1.5;
}
.ps-badge-green { background: rgba(46,204,113,0.1); color: #34d399; border: 1px solid rgba(46,204,113,0.15); }
.ps-badge-blue { background: rgba(74,158,237,0.1); color: #6ba1eb; border: 1px solid rgba(74,158,237,0.15); }
.ps-badge-gold { background: rgba(245,158,11,0.1); color: #f59e0b; border: 1px solid rgba(245,158,11,0.15); }
.ps-badge-red { background: rgba(239,68,68,0.1); color: #ef4444; border: 1px solid rgba(239,68,68,0.15); }
.ps-badge-gray { background: rgba(148,163,184,0.08); color: #94a3b8; border: 1px solid rgba(148,163,184,0.12); }
.ps-badge-purple { background: rgba(168,85,247,0.1); color: #a855f7; border: 1px solid rgba(168,85,247,0.15); }
.ps-badge-orange { background: rgba(251,146,60,0.1); color: #fb923c; border: 1px solid rgba(251,146,60,0.15); }
.ps-badge-top {
    background: rgba(245,158,11,0.12);
    color: #f59e0b;
    border: 1px solid rgba(245,158,11,0.25);
    font-weight: 700;
    font-size: 0.8em;
    padding: 3px 12px;
}
.ps-badge-transp { background: rgba(148,163,184,0.04); color: #94a3b8; border: 1px dashed rgba(148,163,184,0.15); }
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
    color: #8b95a5;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.ps-metric-val {
    font-size: 1.2em;
    font-weight: 700;
    color: #f0f2f5;
}
.ps-quality {
    font-size: 0.82em;
    color: #8b95a5;
    margin-top: 8px;
    line-height: 1.6;
}
.ps-why {
    font-size: 0.78em;
    color: #8b95a5;
    font-style: italic;
    margin-top: 6px;
    padding: 5px 10px;
    background: rgba(255,255,255,0.02);
    border-radius: 4px;
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
    min-width: 64px;
}
.ps-sub-label {
    font-size: 0.65em;
    color: #8b95a5;
    text-transform: uppercase;
}
.ps-link {
    font-size: 0.82em;
    color: #6ba1eb;
    text-decoration: none;
}
.ps-link:hover {
    text-decoration: underline;
    color: #93bbf0;
}
.ps-score-big {
    font-size: 1.4em;
    font-weight: 700;
}

.upgrade-btn {
    display: inline-block;
    padding: 8px 20px;
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 6px;
    color: #6ba1eb;
    font-weight: 500;
    font-size: 0.85em;
    text-decoration: none;
    cursor: pointer;
}

.upgrade-btn:hover {
    background: rgba(255,255,255,0.08);
}

.section-title {
    font-size: 1.1em;
    font-weight: 600;
    color: #f0f2f5;
    margin: 16px 0 12px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}

.landing-navbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 40px;
    height: 56px;
    position: sticky;
    top: 0;
    z-index: 100;
    background: rgba(10, 12, 24, 0.92);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border-bottom: 1px solid rgba(255,255,255,0.06);
    box-shadow: 0 1px 3px rgba(0,0,0,0.3);
}
@media (max-width: 768px) {
    .landing-navbar { padding: 0 16px; }
    .landing-navbar-links { display: none !important; }
}
.landing-navbar-left {
    display: flex;
    align-items: center;
    gap: 10px;
}
.landing-navbar-brand {
    font-size: 1.15em;
    font-weight: 700;
    color: #f0f2f5;
    letter-spacing: -0.3px;
}
.landing-navbar-links {
    display: flex;
    align-items: center;
    gap: 28px;
}
.landing-navbar-links a {
    color: #8b95a5;
    text-decoration: none;
    font-weight: 400;
    font-size: 0.88em;
}
.landing-navbar-links a:hover {
    color: #f0f2f5;
}
.landing-navbar-right {
    display: flex;
    align-items: center;
    gap: 12px;
}
.landing-navbar-btn-ghost {
    padding: 6px 16px;
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.85em;
    color: #c0c8d4;
    text-decoration: none;
    cursor: pointer;
    border: none;
    background: transparent;
}
.landing-navbar-btn-ghost:hover {
    color: #f0f2f5;
}
.landing-navbar-btn-primary {
    padding: 7px 18px;
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.85em;
    color: white;
    background: #2563eb;
    border: none;
    cursor: pointer;
    text-decoration: none;
}
.landing-navbar-btn-primary:hover {
    background: #1d4ed8;
}

.landing-hero {
    padding: 100px 40px 60px 40px;
    max-width: 720px;
    margin: 0 auto;
    text-align: center;
}
@media (max-width: 768px) {
    .landing-hero { padding: 60px 16px 40px 16px; }
    .landing-hero h1 { font-size: 1.8em !important; }
}
.landing-hero h1 {
    font-size: 2.8em;
    font-weight: 700;
    color: #f0f2f5;
    letter-spacing: -1px;
    line-height: 1.15;
    margin-bottom: 16px;
}
.landing-hero h1 span {
    color: #6ba1eb;
}
.landing-hero p {
    font-size: 1.05em;
    color: #8b95a5;
    line-height: 1.7;
    margin-bottom: 36px;
    max-width: 560px;
    margin-left: auto;
    margin-right: auto;
}
.landing-hero-cta {
    display: inline-block;
    padding: 12px 32px;
    border-radius: 8px;
    font-weight: 600;
    font-size: 0.95em;
    color: white;
    background: #2563eb;
    border: none;
    cursor: pointer;
    text-decoration: none;
}
.landing-hero-cta:hover {
    background: #1d4ed8;
}
.landing-hero-login {
    display: block;
    margin-top: 14px;
    font-size: 0.85em;
    color: #8b95a5;
    text-decoration: none;
    cursor: pointer;
}
.landing-hero-login:hover {
    color: #f0f2f5;
}

.landing-social-proof {
    display: flex;
    gap: 40px;
    justify-content: center;
    flex-wrap: wrap;
    padding: 40px 40px 0 40px;
    max-width: 800px;
    margin: 0 auto;
}
@media (max-width: 768px) {
    .landing-social-proof { gap: 20px; padding: 24px 16px 0 16px; }
}
.landing-social-proof-item {
    text-align: center;
}
.landing-social-proof-num {
    font-size: 1.6em;
    font-weight: 700;
    color: #f0f2f5;
}
.landing-social-proof-label {
    font-size: 0.78em;
    color: #8b95a5;
    margin-top: 2px;
}

.landing-cards {
    display: flex;
    gap: 20px;
    justify-content: center;
    flex-wrap: wrap;
    padding: 60px 40px;
    max-width: 960px;
    margin: 0 auto;
}
@media (max-width: 768px) {
    .landing-cards { padding: 32px 16px; flex-direction: column; align-items: center; }
}
.landing-card {
    flex: 1;
    min-width: 240px;
    max-width: 300px;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 28px 24px;
    text-align: left;
}
.landing-card:hover {
    border-color: rgba(255,255,255,0.1);
}
.landing-card-icon {
    font-size: 1.4em;
    margin-bottom: 14px;
    color: #6ba1eb;
}
.landing-card h3 {
    font-size: 1em;
    font-weight: 600;
    color: #f0f2f5;
    margin-bottom: 6px;
}
.landing-card p {
    font-size: 0.85em;
    color: #8b95a5;
    line-height: 1.5;
}
.landing-steps {
    padding: 60px 40px;
    max-width: 880px;
    margin: 0 auto;
    text-align: center;
    background: rgba(255,255,255,0.015);
    border: 1px solid rgba(255,255,255,0.04);
    border-radius: 8px;
    margin-bottom: 40px;
}
@media (max-width: 768px) {
    .landing-steps { padding: 32px 16px; }
}
.landing-steps h2 {
    font-size: 1.6em;
    font-weight: 700;
    color: #f0f2f5;
    margin-bottom: 40px;
}
.landing-steps-grid {
    display: flex;
    gap: 40px;
    justify-content: center;
    flex-wrap: wrap;
}
.landing-step {
    flex: 1;
    min-width: 200px;
    max-width: 240px;
}
.landing-step-num {
    width: 40px;
    height: 40px;
    border-radius: 8px;
    background: #2563eb;
    color: white;
    font-size: 1em;
    font-weight: 700;
    display: flex;
    align-items: center;
    justify-content: center;
    margin: 0 auto 16px auto;
}
.landing-step h4 {
    font-size: 1em;
    font-weight: 600;
    color: #f0f2f5;
    margin-bottom: 6px;
}
.landing-step p {
    font-size: 0.85em;
    color: #8b95a5;
    line-height: 1.5;
}
.landing-footer {
    text-align: center;
    padding: 40px;
    border-top: 1px solid rgba(255,255,255,0.04);
    margin-top: 60px;
}
.landing-footer p {
    color: #555e6e;
    font-size: 0.82em;
    line-height: 1.8;
}
.landing-footer a {
    color: #6ba1eb;
    text-decoration: none;
}
.landing-footer a:hover {
    color: #93bbf0;
}

.compare-bar {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    background: rgba(10, 12, 24, 0.95);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border-top: 1px solid rgba(255,255,255,0.08);
    padding: 14px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    z-index: 999;
}

.section-alt {
    background: rgba(255,255,255,0.015);
    border: 1px solid rgba(255,255,255,0.04);
    border-radius: 8px;
    padding: 24px 22px;
    margin: 16px 0;
}

.admin-section {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 24px;
    margin-bottom: 20px;
}
.admin-section-title {
    font-size: 1.05em;
    font-weight: 600;
    color: #f0f2f5;
    margin-bottom: 4px;
}
.admin-section-desc {
    font-size: 0.82em;
    color: #8b95a5;
    margin-bottom: 16px;
}

.review-card {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    padding: 18px 20px;
    margin-bottom: 12px;
}
.review-card:hover {
    border-color: rgba(255,255,255,0.1);
}
.review-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
}
.review-author {
    font-weight: 600;
    color: #f0f2f5;
    font-size: 0.9em;
}
.review-date {
    color: #555e6e;
    font-size: 0.78em;
}
.review-stars {
    color: #f59e0b;
    font-size: 1em;
    margin-bottom: 6px;
}
.review-title {
    font-weight: 600;
    color: #c0c8d4;
    font-size: 0.9em;
    margin-bottom: 4px;
}
.review-comment {
    color: #8b95a5;
    font-size: 0.88em;
    line-height: 1.6;
}

.product-detail-header {
    display: flex;
    align-items: center;
    gap: 20px;
    margin-bottom: 24px;
    padding: 20px;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
}
.product-detail-score {
    text-align: center;
    min-width: 100px;
}
.product-detail-info h1 {
    font-size: 1.5em;
    font-weight: 700;
    color: #f0f2f5;
    margin-bottom: 4px;
    line-height: 1.2;
}
.product-detail-info .brand {
    color: #8b95a5;
    font-size: 0.95em;
}
.ps-img {
    width: 72px;
    height: 72px;
    object-fit: contain;
    border-radius: 8px;
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.06);
    flex-shrink: 0;
}
.ps-img-placeholder {
    width: 72px;
    height: 72px;
    border-radius: 8px;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.04);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.6em;
    color: rgba(255,255,255,0.1);
    flex-shrink: 0;
}
</style>
"""

CARD_CSS = ""

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

api_key = os.environ.get("BRAVE_API_KEY", "") or os.environ.get("BRAVE_SEARCH_API_KEY", "")

for key in ["user", "page", "selected_product_id"]:
    if key not in st.session_state:
        st.session_state[key] = None
if "compare_list" not in st.session_state:
    st.session_state.compare_list = []
if "page" not in st.session_state or st.session_state.page is None:
    st.session_state.page = "landing"


def logout():
    st.session_state.user = None
    st.session_state.page = "landing"
    st.session_state.selected_product_id = None
    st.session_state.compare_list = []


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
        return "#8b95a5"
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

    image_url = row.get("image_url", "")
    if image_url:
        img_html = f"<img src='{html_module.escape(image_url)}' class='ps-img' alt='' onerror=\"this.style.display='none';this.nextElementSibling.style.display='flex';\" /><div class='ps-img-placeholder' style='display:none;'>🥛</div>"
    else:
        img_html = "<div class='ps-img-placeholder'>🥛</div>"

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

        <div style='flex-shrink:0;text-align:center;'>
          {img_html}
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
    st.html(card_html)




def render_catalog_results(products):
    mapped_products = []
    for p in products:
        mapped_products.append({
            "product_id": p.get("id"),
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

    filter_key = f"{search_query}|{filter_top}|{filter_no_sweetener}|{filter_france}|{filter_clean}|{filter_type}|{sort_option}"
    if st.session_state.get("_cat_filter_key") != filter_key:
        st.session_state._cat_filter_key = filter_key
        st.session_state.cat_page = 0

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

    PRODUCTS_PER_PAGE = 20
    total_products = len(sorted_df)
    total_pages = max(1, (total_products + PRODUCTS_PER_PAGE - 1) // PRODUCTS_PER_PAGE)

    if "cat_page" not in st.session_state:
        st.session_state.cat_page = 0
    current_page = st.session_state.cat_page
    if current_page >= total_pages:
        current_page = 0
        st.session_state.cat_page = 0

    start_idx = current_page * PRODUCTS_PER_PAGE
    end_idx = min(start_idx + PRODUCTS_PER_PAGE, total_products)
    page_df = sorted_df.iloc[start_idx:end_idx]

    pcol1, pcol2, pcol3 = st.columns([2, 3, 2])
    with pcol1:
        st.markdown(f"**{total_products} produits** — page {current_page + 1}/{total_pages}")
    with pcol2:
        nav1, nav2, nav3 = st.columns(3)
        with nav1:
            if st.button("◀ Precedent", disabled=(current_page == 0), key="cat_prev", use_container_width=True):
                st.session_state.cat_page = current_page - 1
                st.rerun()
        with nav2:
            new_page = st.number_input("Page", min_value=1, max_value=total_pages, value=current_page + 1, key="cat_page_input", label_visibility="collapsed")
            if new_page - 1 != current_page:
                st.session_state.cat_page = new_page - 1
                st.rerun()
        with nav3:
            if st.button("Suivant ▶", disabled=(current_page >= total_pages - 1), key="cat_next", use_container_width=True):
                st.session_state.cat_page = current_page + 1
                st.rerun()

    for rank, (idx, row) in enumerate(page_df.iterrows(), start_idx + 1):
        render_product_card_v2(rank, row)

        product_id = row.get("product_id") if "product_id" in row.index else None
        if product_id is None and "id" in row.index:
            product_id = row.get("id")

        if product_id:
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
            with btn_col1:
                if st.button("Voir fiche", key=f"cat_view_{product_id}_{rank}", use_container_width=True):
                    st.session_state.selected_product_id = int(product_id)
                    st.session_state.page = "product"
                    st.rerun()
            with btn_col2:
                compare_ids = st.session_state.compare_list
                if int(product_id) in compare_ids:
                    if st.button("- Retirer", key=f"cat_rmcmp_{product_id}_{rank}", use_container_width=True):
                        st.session_state.compare_list = [x for x in compare_ids if x != int(product_id)]
                        st.rerun()
                elif len(compare_ids) < 5:
                    if st.button("+ Comparer", key=f"cat_addcmp_{product_id}_{rank}", use_container_width=True):
                        st.session_state.compare_list.append(int(product_id))
                        st.rerun()

    if st.session_state.compare_list:
        n = len(st.session_state.compare_list)
        st.markdown(f"""
        <div class='compare-bar'>
            <span style='color:#f0f2f5;font-weight:600;font-size:1em;'>Comparateur ({n} produit{'s' if n > 1 else ''})</span>
        </div>
        """, unsafe_allow_html=True)
        if st.button(f"Ouvrir le comparateur ({n})", type="primary", key="open_compare_bar"):
            st.session_state.page = "compare"
            st.rerun()

    st.divider()
    st.subheader("Tableau complet")

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

    with st.expander("📥 Exporter le catalogue"):
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
                <img src='data:image/png;base64,{LOGO_B64}' style='height:44px;width:auto;' />
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
        st.markdown(f"""
        <div class='sidebar-user'>
            <div class='sidebar-user-name'><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#8b95a5" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px;margin-right:6px;"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>{html_module.escape(user['display_name'])}</div>
            <div class='sidebar-user-email'>{html_module.escape(user['email'])}</div>
            <div class='sidebar-user-plan'>Plan: {plan_label}</div>
        </div>
        """, unsafe_allow_html=True)

        nav_pages = [
            ("catalogue", "Catalogue"),
            ("compare", "Comparateur"),
            ("admin", "Administration"),
        ]

        for page_id, label in nav_pages:
            is_active = current_page == page_id or (page_id == "catalogue" and current_page == "product")
            if st.button(label, use_container_width=True, type="primary" if is_active else "secondary", key=f"nav_{page_id}"):
                st.session_state.page = page_id
                st.rerun()

        st.markdown("---")
        if st.button("Se deconnecter", use_container_width=True):
            logout()
            st.rerun()


# ── LANDING PAGE ──

def page_landing():
    logo_html = ""
    if LOGO_B64:
        logo_html = f"<img src='data:image/png;base64,{LOGO_B64}' style='height:28px;width:auto;' />"

    st.markdown(f"""
    <div class='landing-navbar'>
        <div class='landing-navbar-left'>
            {logo_html}
            <span class='landing-navbar-brand'>ProteinScan</span>
        </div>
        <div class='landing-navbar-links'>
            <a href='#fonctionnalites'>Fonctionnalites</a>
            <a href='#comment-ca-marche'>Comment ca marche</a>
            <a href='#comparateur'>Comparateur</a>
        </div>
        <div class='landing-navbar-right'>
        </div>
    </div>
    """, unsafe_allow_html=True)

    nav_col1, nav_col2, nav_col3 = st.columns([6, 1, 1])
    with nav_col2:
        if st.button("Se connecter", key="header_login"):
            st.session_state.page = "login"
            st.rerun()
    with nav_col3:
        if st.button("Creer un compte", key="header_register", type="primary"):
            st.session_state.page = "register"
            st.rerun()

    stats = cached_get_catalog_stats()
    total_products = stats.get("total_products", 0)
    total_brands = stats.get("unique_brands", 0) if "unique_brands" in stats else "50+"

    st.markdown(f"""
    <div class='landing-hero'>
        <h1>Comparez les whey <span>intelligemment.</span></h1>
        <p>Analyse automatique des macros, aminogramme, score qualite et prix au gramme de proteine.</p>
    </div>
    """, unsafe_allow_html=True)

    col_cta_left, col_cta_mid, col_cta_right = st.columns([2, 2, 2])
    with col_cta_mid:
        if st.button("Commencer gratuitement", type="primary", use_container_width=True, key="hero_cta"):
            st.session_state.page = "register"
            st.rerun()
        if st.button("Se connecter", use_container_width=False, key="hero_login"):
            st.session_state.page = "login"
            st.rerun()

    st.markdown(f"""
    <div class='landing-social-proof'>
        <div class='landing-social-proof-item'>
            <div class='landing-social-proof-num'>+{total_products}</div>
            <div class='landing-social-proof-label'>produits analyses</div>
        </div>
        <div class='landing-social-proof-item'>
            <div class='landing-social-proof-num'>{total_brands}</div>
            <div class='landing-social-proof-label'>marques detectees</div>
        </div>
        <div class='landing-social-proof-item'>
            <div class='landing-social-proof-num'>Quotidien</div>
            <div class='landing-social-proof-label'>prix actualises</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class='landing-cards' id='fonctionnalites'>
        <div class='landing-card'>
            <div class='landing-card-icon'>&#128269;</div>
            <h3>Comparaison instantanee</h3>
            <p>Comparez jusqu'a 5 produits cote a cote.</p>
        </div>
        <div class='landing-card'>
            <div class='landing-card-icon'>&#11088;</div>
            <h3>Score qualite/prix</h3>
            <p>Algorithme transparent base sur proteines, ingredients et cout.</p>
        </div>
        <div class='landing-card'>
            <div class='landing-card-icon'>&#128200;</div>
            <h3>Historique des prix</h3>
            <p>Suivi automatique des promotions.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class='landing-steps' id='comment-ca-marche'>
        <h2>Comment ca marche</h2>
        <div class='landing-steps-grid'>
            <div class='landing-step'>
                <div class='landing-step-num'>1</div>
                <h4>Chercher</h4>
                <p>Parcourez le catalogue ou lancez une recherche pour decouvrir les meilleures wheys.</p>
            </div>
            <div class='landing-step'>
                <div class='landing-step-num'>2</div>
                <h4>Comparer</h4>
                <p>Ajoutez vos favoris au comparateur et analysez les differences.</p>
            </div>
            <div class='landing-step'>
                <div class='landing-step-num'>3</div>
                <h4>Decider</h4>
                <p>Consultez les scores et les avis, choisissez le meilleur produit.</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    col_b1, col_b2 = st.columns(2)
    with col_b1:
        if st.button("Creer un compte gratuitement", type="primary", use_container_width=True, key="landing_register"):
            st.session_state.page = "register"
            st.rerun()
    with col_b2:
        if st.button("J'ai deja un compte", use_container_width=True, key="landing_login"):
            st.session_state.page = "login"
            st.rerun()

    st.markdown("""
    <div class='landing-footer'>
        <p>ProteinScan &copy; 2025 &mdash; Comparateur de proteines whey en France</p>
        <p><a href='#'>Mentions legales</a> &middot; <a href='#'>CGU</a> &middot; <a href='#'>Contact</a></p>
    </div>
    """, unsafe_allow_html=True)


# ── AUTH PAGES ──

def page_login():
    if LOGO_B64:
        st.markdown(f"""
        <div style='text-align:center;padding:40px 0 10px 0;'>
            <img src='data:image/png;base64,{LOGO_B64}' style='height:120px;width:auto;' />
        </div>
        <div style='text-align:center;color:#6b85b0;font-size:1em;margin-bottom:24px;letter-spacing:0.3px;'>
            Comparateur de proteines whey en France
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='max-width:420px;margin:0 auto;'>", unsafe_allow_html=True)
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
                    st.session_state.page = "catalogue"
                    st.session_state.view_scan_id = None
                    st.rerun()
                else:
                    st.error("Email ou mot de passe incorrect.")

    st.markdown("</div>", unsafe_allow_html=True)

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if st.button("Pas de compte ? Creer un compte", use_container_width=True, key="switch_to_register"):
            st.session_state.page = "register"
            st.rerun()
    with col_s2:
        if st.button("Retour a l'accueil", use_container_width=True, key="back_landing_from_login"):
            st.session_state.page = "landing"
            st.rerun()


def page_register():
    if LOGO_B64:
        st.markdown(f"""
        <div style='text-align:center;padding:40px 0 10px 0;'>
            <img src='data:image/png;base64,{LOGO_B64}' style='height:120px;width:auto;' />
        </div>
        <div style='text-align:center;color:#6b85b0;font-size:1em;margin-bottom:24px;letter-spacing:0.3px;'>
            Comparateur de proteines whey en France
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='max-width:420px;margin:0 auto;'>", unsafe_allow_html=True)
    st.subheader("Creer un compte")
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
                    st.session_state.page = "catalogue"
                    st.session_state.view_scan_id = None
                    st.rerun()
                else:
                    st.error("Cet email est deja utilise.")

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("""
    <div style='text-align:center;margin-top:16px;padding:12px 20px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:8px;color:#8b95a5;font-size:0.85em;max-width:420px;margin-left:auto;margin-right:auto;'>
        <span style='color:#6ba1eb;font-weight:700;'>Plan Gratuit</span> : 3 scans par mois &nbsp;|&nbsp;
        <span style='color:#6ba1eb;font-weight:700;'>Plan Pro</span> : scans illimites (bientot disponible)
    </div>
    """, unsafe_allow_html=True)

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if st.button("Deja un compte ? Se connecter", use_container_width=True, key="switch_to_login"):
            st.session_state.page = "login"
            st.rerun()
    with col_s2:
        if st.button("Retour a l'accueil", use_container_width=True, key="back_landing_from_register"):
            st.session_state.page = "landing"
            st.rerun()


# ── CATALOGUE PAGE ──

def page_catalogue():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Catalogue")

    products = cached_get_all_products(min_confidence=0.3, limit=300)

    scored_products = [p for p in products if p.get("score_final") is not None]

    if not scored_products:
        st.info("Aucun produit dans le catalogue pour le moment.")
        return

    render_catalog_results(scored_products)


# ── COMPARE PAGE ──

def page_compare():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Comparateur")

    compare_ids = st.session_state.compare_list
    if not compare_ids:
        st.info("Aucun produit dans le comparateur. Ajoutez des produits depuis le Catalogue.")
        if st.button("Aller au catalogue", type="primary"):
            st.session_state.page = "catalogue"
            st.rerun()
        return

    products = cached_get_products_by_ids(tuple(compare_ids))
    if not products:
        st.warning("Les produits selectionnes n'ont pas ete trouves.")
        st.session_state.compare_list = []
        return

    st.markdown(f"**{len(products)} produit(s) en comparaison** (max 5)")

    if st.button("Vider le comparateur", key="clear_compare"):
        st.session_state.compare_list = []
        st.rerun()

    cols = st.columns(len(products))
    for i, p in enumerate(products):
        with cols[i]:
            name = p.get("name", "Produit")
            brand = p.get("brand", "")
            s_final = p.get("score_final")
            s_prot = p.get("score_proteique")
            s_sante = p.get("score_sante")
            prix_kg = p.get("offer_prix_par_kg")
            prot = p.get("proteines_100g")
            poids = p.get("offer_poids_kg")
            prix = p.get("offer_prix")
            type_whey = p.get("type_whey", "unknown")
            origin = p.get("origin_label", "Inconnu")
            leucine = p.get("leucine_g")
            bcaa = p.get("bcaa_per_100g_prot")
            ingr_count = p.get("ingredient_count")
            has_sucr = p.get("has_sucralose", False)
            has_ace = p.get("has_acesulfame_k", False)
            url = p.get("offer_url", "")

            score_display = f"{s_final:.1f}/10" if is_valid(s_final) else "N/A"
            color = score_color_10(s_final)

            st.markdown(f"""
            <div style='text-align:center;padding:16px 8px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:8px;margin-bottom:12px;'>
                <div style='font-size:0.85em;color:#8b95a5;'>{html_module.escape(brand)}</div>
                <div style='font-size:1em;font-weight:700;color:#f0f2f5;margin:4px 0;'>{html_module.escape(name[:60])}</div>
                <div style='font-size:1.6em;font-weight:800;color:{color};'>{score_display}</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("**Donnees cles**")
            st.markdown(f"- **Type** : {type_whey.capitalize() if type_whey != 'unknown' else 'N/D'}")
            st.markdown(f"- **Prot/100g** : {prot:.1f}g" if is_valid(prot) else "- **Prot/100g** : N/D")
            st.markdown(f"- **Prix/kg** : {prix_kg:.0f} EUR" if is_valid(prix_kg) else "- **Prix/kg** : N/D")
            st.markdown(f"- **Prix** : {prix:.2f} EUR" if is_valid(prix) else "- **Prix** : N/D")
            st.markdown(f"- **Poids** : {poids:.2f} kg" if is_valid(poids) else "- **Poids** : N/D")

            cout_30g = None
            if is_valid(prot) and is_valid(prix_kg) and prot > 0:
                cout_30g = (30 / prot) * (prix_kg / 10)
            st.markdown(f"- **Cout 30g prot** : {cout_30g:.2f} EUR" if cout_30g else "- **Cout 30g prot** : N/D")

            st.markdown(f"- **BCAA/100g prot** : {bcaa:.1f}g" if is_valid(bcaa) else "- **BCAA** : N/D")
            st.markdown(f"- **Leucine** : {leucine:.1f}g" if is_valid(leucine) else "- **Leucine** : N/D")
            st.markdown(f"- **Origine** : {origin}")
            st.markdown(f"- **Ingredients** : {ingr_count}" if is_valid(ingr_count) else "- **Ingredients** : N/D")

            eduls = []
            if has_sucr: eduls.append("Sucralose")
            if has_ace: eduls.append("Ace-K")
            st.markdown(f"- **Edulcorants** : {', '.join(eduls) if eduls else 'Aucun'}")

            st.markdown("**Scores**")
            st.markdown(f"- Note finale : **{s_final:.1f}/10**" if is_valid(s_final) else "- Note finale : N/D")
            st.markdown(f"- Proteique : **{s_prot:.1f}/10**" if is_valid(s_prot) else "- Proteique : N/D")
            st.markdown(f"- Sante : **{s_sante:.1f}/10**" if is_valid(s_sante) else "- Sante : N/D")

            if url:
                st.markdown(f"[Voir le produit]({url})")

            if st.button("Retirer", key=f"remove_compare_{p['id']}", use_container_width=True):
                st.session_state.compare_list = [x for x in st.session_state.compare_list if x != p["id"]]
                st.rerun()

            if st.button("Voir fiche", key=f"goto_product_{p['id']}", use_container_width=True):
                st.session_state.selected_product_id = p["id"]
                st.session_state.page = "product"
                st.rerun()


# ── PRODUCT DETAIL PAGE ──

def page_product():
    user = st.session_state.user
    render_sidebar()

    product_id = st.session_state.selected_product_id
    if not product_id:
        st.warning("Aucun produit selectionne.")
        if st.button("Retour au catalogue"):
            st.session_state.page = "catalogue"
            st.rerun()
        return

    product = cached_get_product_by_id(product_id)
    if not product:
        st.error("Produit introuvable.")
        if st.button("Retour au catalogue"):
            st.session_state.page = "catalogue"
            st.rerun()
        return

    if st.button("Retour au catalogue"):
        st.session_state.page = "catalogue"
        st.rerun()

    name = product.get("name", "Produit inconnu")
    brand = product.get("brand", "")
    s_final = product.get("score_final")
    s_prot = product.get("score_proteique")
    s_sante = product.get("score_sante")
    type_whey = product.get("type_whey", "unknown")
    origin = product.get("origin_label", "Inconnu")
    prot = product.get("proteines_100g")
    leucine = product.get("leucine_g")
    bcaa = product.get("bcaa_per_100g_prot")
    ingr_count = product.get("ingredient_count")
    ingredients = product.get("ingredients", "")

    color = score_color_10(s_final)
    score_display = f"{s_final:.1f}/10" if is_valid(s_final) else "N/A"

    product_image_url = product.get("image_url", "")
    if product_image_url:
        product_img_html = f"<img src='{html_module.escape(product_image_url)}' style='width:120px;height:120px;object-fit:contain;border-radius:8px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.06);' alt='' onerror=\"this.style.display='none';this.nextElementSibling.style.display='flex';\" /><div class='ps-img-placeholder' style='display:none;width:120px;height:120px;font-size:3em;border-radius:8px;'>🥛</div>"
    else:
        product_img_html = "<div class='ps-img-placeholder' style='width:120px;height:120px;font-size:3em;border-radius:8px;'>🥛</div>"

    st.markdown(f"""
    <div class='product-detail-header'>
        <div class='product-detail-score'>
            <div style='font-size:2.2em;font-weight:800;color:{color};'>{score_display}</div>
            <div style='font-size:0.8em;color:#8b95a5;'>Note finale</div>
        </div>
        <div style='flex-shrink:0;'>
            {product_img_html}
        </div>
        <div class='product-detail-info'>
            <h1>{html_module.escape(name)}</h1>
            <div class='brand'>{html_module.escape(brand)}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    mapped = {
        "nom": name,
        "marque": brand,
        "proteines_100g": prot,
        "type_whey": type_whey,
        "origin_label": origin,
        "has_sucralose": product.get("has_sucralose", False),
        "has_acesulfame_k": product.get("has_acesulfame_k", False),
        "has_aspartame": product.get("has_aspartame", False),
        "has_aminogram": product.get("has_aminogram", False),
        "mentions_bcaa": product.get("mentions_bcaa", False),
        "has_artificial_flavors": product.get("has_artificial_flavors", False),
        "has_thickeners": product.get("has_thickeners", False),
        "has_colorants": product.get("has_colorants", False),
        "ingredient_count": ingr_count,
        "bcaa_per_100g_prot": bcaa,
        "leucine_g": leucine,
        "profil_suspect": product.get("profil_suspect", False),
        "protein_suspect": product.get("protein_suspect", False),
        "score_proteique": s_prot,
        "score_sante": s_sante,
        "score_final": s_final,
        "score_global": product.get("score_global"),
        "image_url": product.get("image_url"),
    }
    whey_badge = get_whey_badge(type_whey)
    origin_badge = get_origin_badge(origin)
    sweetener_badge = get_sweetener_badges(
        product.get("has_sucralose", False),
        product.get("has_acesulfame_k", False),
        product.get("has_aspartame", False),
    )
    st.markdown(f"<div style='margin:8px 0 20px 0;'>{whey_badge} {origin_badge} {sweetener_badge}</div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Note proteique", f"{s_prot:.1f}/10" if is_valid(s_prot) else "N/D")
    with col2:
        st.metric("Note sante", f"{s_sante:.1f}/10" if is_valid(s_sante) else "N/D")
    with col3:
        price_score = calculate_price_score_10(None)
        offers = cached_get_product_offers(product_id)
        if offers:
            best = offers[0]
            price_score = calculate_price_score_10(best.get("prix_par_kg"))
        st.metric("Note prix", f"{price_score:.1f}/10" if is_valid(price_score) else "N/D")

    st.divider()

    detail_col, offers_col = st.columns([1, 1])

    with detail_col:
        st.subheader("Details nutritionnels")
        st.markdown(f"- **Proteines / 100g** : {prot:.1f}g" if is_valid(prot) else "- **Proteines / 100g** : N/D")

        kcal = product.get("kcal_per_100g")
        carbs = product.get("carbs_per_100g")
        sugar = product.get("sugar_per_100g")
        fat = product.get("fat_per_100g")
        sat_fat = product.get("sat_fat_per_100g")
        salt = product.get("salt_per_100g")
        fiber = product.get("fiber_per_100g")

        if is_valid(kcal):
            st.markdown(f"- **Calories** : {kcal:.0f} kcal")
        if is_valid(carbs):
            sugar_txt = f" (dont sucres {sugar:.1f}g)" if is_valid(sugar) else ""
            st.markdown(f"- **Glucides** : {carbs:.1f}g{sugar_txt}")
        if is_valid(fat):
            sat_txt = f" (dont sat. {sat_fat:.1f}g)" if is_valid(sat_fat) else ""
            st.markdown(f"- **Lipides** : {fat:.1f}g{sat_txt}")
        if is_valid(salt):
            st.markdown(f"- **Sel** : {salt:.2f}g")
        if is_valid(fiber):
            st.markdown(f"- **Fibres** : {fiber:.1f}g")

        st.markdown(f"- **BCAA / 100g prot** : {bcaa:.1f}g" if is_valid(bcaa) else "- **BCAA / 100g prot** : N/D")
        st.markdown(f"- **Leucine** : {leucine:.1f}g" if is_valid(leucine) else "- **Leucine** : N/D")
        st.markdown(f"- **Type de whey** : {type_whey.capitalize() if type_whey != 'unknown' else 'Non determine'}")
        st.markdown(f"- **Origine** : {origin}")
        st.markdown(f"- **Nombre d'ingredients** : {ingr_count}" if is_valid(ingr_count) else "- **Ingredients** : N/D")

        macro_ok = product.get("macro_coherent")
        if macro_ok is False:
            st.warning("Coherence macro suspecte (kcal vs macros)")
        sources = product.get("nutrition_sources", "")
        if sources:
            st.caption(f"Sources: {sources}")

        amino_full = product.get("amino_profile")
        if amino_full and isinstance(amino_full, dict) and len(amino_full) > 0:
            with st.expander(f"Aminogramme complet ({len(amino_full)} acides amines)"):
                amino_base_label = product.get("amino_base", "unknown")
                base_labels = {"per_100g_protein": "pour 100g de proteines", "per_100g": "pour 100g", "per_serving": "par dose"}
                st.caption(f"Base: {base_labels.get(amino_base_label, amino_base_label)}")
                amino_names_fr = {
                    "leucine": "Leucine", "isoleucine": "Isoleucine", "valine": "Valine",
                    "glutamine": "Glutamine", "arginine": "Arginine", "lysine": "Lysine",
                    "methionine": "Methionine", "phenylalanine": "Phenylalanine",
                    "threonine": "Threonine", "tryptophan": "Tryptophane",
                    "histidine": "Histidine", "alanine": "Alanine", "glycine": "Glycine",
                    "proline": "Proline", "serine": "Serine", "tyrosine": "Tyrosine",
                    "aspartic_acid": "Acide aspartique", "cysteine": "Cysteine",
                }
                for k, v in sorted(amino_full.items(), key=lambda x: -x[1] if isinstance(x[1], (int, float)) else 0):
                    label = amino_names_fr.get(k, k.replace("_", " ").capitalize())
                    if isinstance(v, (int, float)):
                        st.markdown(f"- **{label}** : {v:.2f}g")

        if ingredients:
            with st.expander("Liste des ingredients"):
                st.text(ingredients)

    with offers_col:
        st.subheader("Offres disponibles")
        offers = cached_get_product_offers(product_id)
        if not offers:
            st.info("Aucune offre active pour ce produit.")
        else:
            for o in offers:
                if not o.get("is_active", True):
                    continue
                prix_o = o.get("prix")
                pkg_o = o.get("prix_par_kg")
                merch = o.get("merchant", "Marchand")
                url_o = o.get("url", "")
                conf = o.get("confidence", 0)
                st.markdown(f"""
                <div style='background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:14px 16px;margin-bottom:8px;'>
                    <div style='font-weight:600;color:#f0f2f5;font-size:0.95em;'>{html_module.escape(str(merch))}</div>
                    <div style='color:#6ba1eb;font-size:1.1em;font-weight:700;margin:4px 0;'>{f'{prix_o:.2f} EUR' if prix_o else 'Prix N/D'} {f'({pkg_o:.0f} EUR/kg)' if pkg_o else ''}</div>
                    <div style='font-size:0.8em;color:#8b95a5;'>Confiance: {conf:.0%}</div>
                    {'<a href="' + url_o + '" target="_blank" style="color:#6ba1eb;font-size:0.85em;">Voir l offre</a>' if url_o else ''}
                </div>
                """, unsafe_allow_html=True)

    compare_ids = st.session_state.compare_list
    if product_id in compare_ids:
        if st.button("Retirer du comparateur", use_container_width=True, key="prod_remove_compare"):
            st.session_state.compare_list = [x for x in compare_ids if x != product_id]
            st.rerun()
    else:
        if len(compare_ids) < 5:
            if st.button("Ajouter au comparateur", type="primary", use_container_width=True, key="prod_add_compare"):
                st.session_state.compare_list.append(product_id)
                st.rerun()

    st.divider()

    st.subheader("Avis de la communaute")

    rating_data = cached_get_average_rating(product_id)
    avg_r = rating_data["average"]
    count_r = rating_data["count"]

    if count_r > 0:
        stars_str = ""
        for i in range(1, 6):
            if avg_r >= i:
                stars_str += "<span style='color:#f59e0b;font-size:1.3em;'>&#9733;</span>"
            elif avg_r >= i - 0.5:
                stars_str += "<span style='color:#f59e0b;font-size:1.3em;'>&#9733;</span>"
            else:
                stars_str += "<span style='color:#3a4a6a;font-size:1.3em;'>&#9733;</span>"
        st.markdown(f"<div>{stars_str} <span style='color:#f0f2f5;font-weight:600;'>{avg_r:.1f}/5</span> <span style='color:#8b95a5;font-size:0.9em;'>({count_r} avis)</span></div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='color:#8b95a5;'>Aucun avis pour le moment. Soyez le premier !</div>", unsafe_allow_html=True)

    with st.expander("Ecrire un avis", expanded=count_r == 0):
        with st.form("review_form"):
            review_rating = st.slider("Note", 1, 5, 4, key="review_rating")
            review_title = st.text_input("Titre (optionnel)", key="review_title")
            review_comment = st.text_area("Commentaire", key="review_comment", placeholder="Partagez votre experience avec ce produit...")
            review_purchased = st.text_input("Achete sur... (optionnel)", key="review_purchased", placeholder="Ex: nutrimuscle.com")
            review_submit = st.form_submit_button("Publier mon avis", use_container_width=True)

            if review_submit:
                if not review_comment.strip():
                    st.error("Veuillez ecrire un commentaire.")
                else:
                    result = create_review(
                        product_id=product_id,
                        user_id=user["id"],
                        rating=review_rating,
                        title=review_title.strip(),
                        comment=review_comment.strip(),
                        purchased_from=review_purchased.strip(),
                    )
                    if result:
                        st.success("Avis publie !")
                        cached_get_reviews.clear()
                        cached_get_average_rating.clear()
                        st.rerun()
                    else:
                        st.error("Erreur lors de la publication.")

    reviews = cached_get_reviews(product_id)
    if reviews:
        sort_reviews = st.selectbox("Trier par", ["Plus recents", "Les mieux notes"], key="review_sort")
        if sort_reviews == "Les mieux notes":
            reviews = sorted(reviews, key=lambda r: r.get("rating", 0), reverse=True)

        for rev in reviews:
            rev_stars = "".join(["<span style='color:#f59e0b;'>&#9733;</span>" if j < rev.get("rating", 0) else "<span style='color:#3a4a6a;'>&#9733;</span>" for j in range(5)])
            rev_date = rev.get("created_at", "")
            if hasattr(rev_date, "strftime"):
                rev_date = rev_date.strftime("%d/%m/%Y")
            rev_author = html_module.escape(rev.get("display_name", "Utilisateur"))
            rev_title = html_module.escape(rev.get("title", ""))
            rev_text = html_module.escape(rev.get("comment", ""))
            rev_purchased = rev.get("purchased_from", "")

            purchased_html = f"<div style='font-size:0.8em;color:#8b95a5;margin-top:4px;'>Achete sur : {html_module.escape(rev_purchased)}</div>" if rev_purchased else ""

            st.markdown(f"""
            <div class='review-card'>
                <div class='review-header'>
                    <div class='review-author'>{rev_author}</div>
                    <div class='review-date'>{rev_date}</div>
                </div>
                <div class='review-stars'>{rev_stars}</div>
                {'<div class="review-title">' + rev_title + '</div>' if rev_title else ''}
                <div class='review-comment'>{rev_text}</div>
                {purchased_html}
            </div>
            """, unsafe_allow_html=True)

            if st.button("Signaler", key=f"flag_review_{rev['id']}", type="secondary"):
                flag_review(rev["id"])
                st.info("Avis signale. Merci.")


# ── ADMIN PAGE ──

def page_admin():
    user = st.session_state.user
    render_sidebar()

    render_page_header("Administration")

    stats = cached_get_catalog_stats()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Produits", stats["total_products"])
    with col2:
        st.metric("Offres actives", stats["total_active_offers"])
    with col3:
        st.metric("Confiance moy.", f"{stats['avg_confidence']:.0%}")
    with col4:
        st.metric("A revoir", stats["products_needing_review"])

    st.markdown("")

    dq = get_data_quality_stats()
    st.markdown("<div class='admin-section'><div class='admin-section-title'>Qualite des donnees</div><div class='admin-section-desc'>Etat de completude du catalogue.</div>", unsafe_allow_html=True)
    dq_total = dq["total"]
    if dq_total > 0:
        dq_c1, dq_c2, dq_c3, dq_c4, dq_c5 = st.columns(5)
        with dq_c1:
            pct_complete = dq["complete"] * 100 // dq_total
            st.metric("Complets", f"{dq['complete']}/{dq_total}", f"{pct_complete}%")
        with dq_c2:
            st.metric("Sans proteines", dq["no_protein"])
        with dq_c3:
            st.metric("Sans ingredients", dq["no_ingredients"])
        with dq_c4:
            st.metric("Sans image", dq["no_image"])
        with dq_c5:
            st.metric("Sans score", dq["no_score"])

        if dq["no_score"] > 0 or dq["no_protein"] > 0:
            with st.expander("Produits incomplets"):
                incomplete = get_incomplete_products_for_rescrape(limit=20)
                if incomplete:
                    inc_data = []
                    for p in incomplete:
                        missing = []
                        if not p.get("proteines_100g"):
                            missing.append("prot")
                        if not p.get("ingredients") or p.get("ingredients") == "[]":
                            missing.append("ingr")
                        if not p.get("image_url"):
                            missing.append("img")
                        if p.get("score_final") is None:
                            missing.append("score")
                        inc_data.append({
                            "Produit": (p.get("name") or "")[:50],
                            "Marque": p.get("brand", ""),
                            "Marchand": p.get("merchant", ""),
                            "Manquant": ", ".join(missing),
                        })
                    st.dataframe(pd.DataFrame(inc_data), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    cleanup_col, rescrape_col = st.columns(2)
    with cleanup_col:
        st.markdown("<div class='admin-section'><div class='admin-section-title'>Nettoyage catalogue</div><div class='admin-section-desc'>Supprime les entrees invalides (pages categorie, marques sans donnees).</div>", unsafe_allow_html=True)
        if st.button("Nettoyer le catalogue", key="btn_cleanup", type="primary"):
            with st.spinner("Nettoyage en cours..."):
                result = cleanup_catalog()
            if result["removed_count"] > 0:
                st.success(f"{result['removed_count']} entrees supprimees.")
                with st.expander("Details"):
                    for n in result["removed_names"]:
                        st.text(f"- {n}")
                cached_get_all_products.clear()
                cached_get_catalog_stats.clear()
            else:
                st.info("Aucune entree a nettoyer.")
        st.markdown("</div>", unsafe_allow_html=True)

    with rescrape_col:
        st.markdown("<div class='admin-section'><div class='admin-section-title'>Re-scrape incomplets</div><div class='admin-section-desc'>Re-scrape les produits avec donnees manquantes (proteines, ingredients, images).</div>", unsafe_allow_html=True)
        rescrape_limit = st.number_input("Nombre de produits", min_value=1, max_value=100, value=20, key="rescrape_limit")
        use_browser = st.checkbox("Utiliser le navigateur (Playwright)", value=False, key="rescrape_browser", help="Plus lent mais extrait les donnees cachees dans les accordeons, onglets et contenu JS.")
        if st.button("Lancer le re-scrape", key="btn_rescrape", type="primary"):
            incomplete_products = get_incomplete_products_for_rescrape(limit=rescrape_limit)
            if not incomplete_products:
                st.info("Tous les produits ont des donnees completes.")
            else:
                progress_rs = st.progress(0)
                status_rs = st.empty()
                detail_rs = st.empty()
                updated = 0
                errors = 0
                browser_used = 0
                for i, prod in enumerate(incomplete_products):
                    progress_rs.progress((i + 1) / len(incomplete_products))
                    mode = " [navigateur]" if use_browser else ""
                    detail_rs.text(f"Re-scrape{mode}: {prod.get('name', '')[:50]}...")
                    try:
                        from scraper import extract_product_data, split_product_offer, compute_confidence_v2
                        from db import upsert_product, upsert_offer
                        result = extract_product_data(prod["offer_url"], force_browser=use_browser)
                        if result:
                            if result.get("_used_browser"):
                                browser_used += 1
                            conf = compute_confidence_v2(result, has_jsonld=bool(result.get("_has_jsonld")), needs_js_render=result.get("_needs_js_render", False))
                            if conf >= 0.2:
                                product_data, offer_data = split_product_offer(result)
                                upsert_product(product_data)
                                updated += 1
                    except Exception as e:
                        errors += 1
                    import time
                    time.sleep(2 if use_browser else 1)
                progress_rs.empty()
                detail_rs.empty()
                browser_info = f", {browser_used} via navigateur" if browser_used else ""
                status_rs.success(f"Re-scrape termine: {updated} mis a jour, {errors} erreurs{browser_info}.")
                cached_get_all_products.clear()
                cached_get_catalog_stats.clear()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    disc_col, refresh_col = st.columns(2)

    with disc_col:
        st.markdown("<div class='admin-section'><div class='admin-section-title'>Discovery</div><div class='admin-section-desc'>Recherche de nouveaux produits whey via Brave Search.</div>", unsafe_allow_html=True)

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
        st.markdown("</div>", unsafe_allow_html=True)

    with refresh_col:
        st.markdown("<div class='admin-section'><div class='admin-section-title'>Refresh</div><div class='admin-section-desc'>Met a jour les prix et la disponibilite des offres existantes.</div>", unsafe_allow_html=True)

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
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Re-analyse nutritionnelle</div><div class='admin-section-desc'>Extraction de l'aminogramme complet, macros etendus et donnees manquantes via le pipeline multi-source (HTML + OCR).</div>", unsafe_allow_html=True)

    from db import get_connection
    _conn_check = get_connection()
    try:
        _cur_check = _conn_check.cursor()
        _cur_check.execute("SELECT COUNT(*) FROM products WHERE amino_profile IS NULL OR kcal_per_100g IS NULL")
        _missing_count = _cur_check.fetchone()[0]
        _cur_check.close()
    finally:
        _conn_check.close()

    st.info(f"{_missing_count} produit(s) sans aminogramme complet ou macros etendus.")

    if _missing_count > 0:
        if st.button("🧬 Lancer la re-analyse", type="primary", use_container_width=True, key="btn_reanalysis"):
            from scraper import run_reanalysis
            status_ra = st.empty()
            progress_ra = st.progress(0)
            detail_ra = st.empty()

            def ra_progress(current, total, detail=""):
                if total > 0:
                    progress_ra.progress(min(current / total, 1.0))
                detail_ra.text(detail)

            def ra_status(msg):
                status_ra.info(msg)

            try:
                with st.spinner("Re-analyse en cours..."):
                    ra_result = run_reanalysis(
                        progress_callback=ra_progress,
                        status_callback=ra_status,
                    )
                progress_ra.empty()
                detail_ra.empty()
                status_ra.success(
                    f"Re-analyse terminee ! "
                    f"Mis a jour: {ra_result.get('updated', 0)}, "
                    f"Echoues: {ra_result.get('failed', 0)}, "
                    f"Ignores: {ra_result.get('skipped', 0)}"
                )
            except Exception as e:
                progress_ra.empty()
                detail_ra.empty()
                status_ra.error(f"Erreur re-analyse : {e}")
    else:
        st.success("Tous les produits ont deja leur aminogramme et macros complets.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Historique des pipelines</div><div class='admin-section-desc'>Dernieres executions des pipelines de donnees.</div>", unsafe_allow_html=True)

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

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Moderation des avis</div><div class='admin-section-desc'>Avis signales par les utilisateurs.</div>", unsafe_allow_html=True)
    flagged = get_flagged_reviews()
    if not flagged:
        st.info("Aucun avis signale.")
    else:
        st.warning(f"{len(flagged)} avis signale(s)")
        for rev in flagged:
            rev_date = rev.get("created_at", "")
            if hasattr(rev_date, "strftime"):
                rev_date = rev_date.strftime("%d/%m/%Y %H:%M")
            st.markdown(f"""
            <div class='review-card'>
                <div style='font-weight:600;color:#f0f2f5;'>{html_module.escape(rev.get('product_name', ''))}</div>
                <div style='color:#8b95a5;font-size:0.85em;'>Par {html_module.escape(rev.get('display_name', ''))} - {rev_date}</div>
                <div style='color:#f59e0b;margin:4px 0;'>{'&#9733;' * rev.get('rating', 0)}{'&#9734;' * (5 - rev.get('rating', 0))}</div>
                <div style='color:#c0c8d4;'>{html_module.escape(rev.get('comment', ''))}</div>
            </div>
            """, unsafe_allow_html=True)
            if st.button(f"Masquer cet avis", key=f"hide_rev_{rev['id']}"):
                hide_review(rev["id"])
                st.success("Avis masque.")
                st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Discovery Health</div><div class='admin-section-desc'>Couverture du catalogue : domaines, marques detectees, marques manquantes.</div>", unsafe_allow_html=True)

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
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Debug : Validateur de page</div><div class='admin-section-desc'>Testez si une URL sera acceptee ou rejetee par le validateur strict.</div>", unsafe_allow_html=True)

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

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    st.markdown("<div class='admin-section'><div class='admin-section-title'>Debug : Resolveur d'URL whey</div><div class='admin-section-desc'>Testez la resolution de liens : si l'URL n'est pas une page produit whey, le resolveur cherche le meilleur lien produit.</div>", unsafe_allow_html=True)

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
    st.markdown("</div>", unsafe_allow_html=True)


# ── ROUTER ──

page = st.session_state.page

if st.session_state.user is None:
    if page == "login":
        page_login()
    elif page == "register":
        page_register()
    else:
        page_landing()
else:
    if page == "catalogue":
        page_catalogue()
    elif page == "compare":
        page_compare()
    elif page == "product":
        page_product()
    elif page == "admin":
        page_admin()
    elif page in ("login", "register", "landing", "dashboard", "search", "scan"):
        st.session_state.page = "catalogue"
        st.rerun()
    else:
        page_catalogue()
