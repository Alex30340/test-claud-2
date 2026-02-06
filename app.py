import streamlit as st
import pandas as pd
import os
import html as html_module
from datetime import datetime

from scraper import scrape_products, extract_product_data, BraveAPIError, SEARCH_QUERIES
from scoring import calculate_price_score
from db import (
    init_db, create_user, get_user_by_email,
    check_and_reset_monthly_usage, increment_scan_count, get_scan_limit,
    save_scan, get_user_scans, get_scan_items, can_user_scan,
)
from auth import hash_password, verify_password

init_db()

st.set_page_config(
    page_title="ProteinScan - Comparateur Whey",
    page_icon="💪",
    layout="wide",
)

CARD_CSS = """
<style>
.ps-card {
    border: 1px solid rgba(100,100,120,0.3);
    border-radius: 14px;
    padding: 18px 22px;
    margin-bottom: 12px;
    background: linear-gradient(135deg, rgba(25,25,35,0.6), rgba(15,15,25,0.3));
    transition: box-shadow 0.2s;
}
.ps-card:hover {
    box-shadow: 0 4px 20px rgba(100,100,255,0.08);
}
.ps-rank {
    font-size: 1.6em;
    font-weight: 800;
    text-align: center;
    line-height: 1.1;
}
.ps-stars {
    color: #f1c40f;
    font-size: 1.3em;
    letter-spacing: 1px;
}
.ps-stars-sm {
    color: #f1c40f;
    font-size: 0.95em;
}
.ps-score-num {
    font-size: 0.85em;
    color: #aaa;
    margin-left: 4px;
}
.ps-title {
    font-size: 1.1em;
    font-weight: 700;
    margin-bottom: 2px;
    line-height: 1.3;
}
.ps-brand {
    font-size: 0.85em;
    color: #999;
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
.ps-badge-green { background: rgba(46,204,113,0.18); color: #2ecc71; border: 1px solid rgba(46,204,113,0.3); }
.ps-badge-blue { background: rgba(52,152,219,0.18); color: #3498db; border: 1px solid rgba(52,152,219,0.3); }
.ps-badge-gold { background: rgba(241,196,15,0.18); color: #f1c40f; border: 1px solid rgba(241,196,15,0.3); }
.ps-badge-red { background: rgba(231,76,60,0.18); color: #e74c3c; border: 1px solid rgba(231,76,60,0.3); }
.ps-badge-gray { background: rgba(150,150,160,0.15); color: #999; border: 1px solid rgba(150,150,160,0.3); }
.ps-badge-purple { background: rgba(155,89,182,0.18); color: #9b59b6; border: 1px solid rgba(155,89,182,0.3); }
.ps-badge-orange { background: rgba(243,156,18,0.18); color: #f39c12; border: 1px solid rgba(243,156,18,0.3); }
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
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.ps-metric-val {
    font-size: 1.3em;
    font-weight: 700;
}
.ps-quality {
    font-size: 0.82em;
    color: #bbb;
    margin-top: 6px;
    line-height: 1.6;
}
.ps-why {
    font-size: 0.78em;
    color: #8e8ea0;
    font-style: italic;
    margin-top: 4px;
    padding: 4px 8px;
    background: rgba(100,100,120,0.1);
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
    color: #888;
    text-transform: uppercase;
}
.ps-link {
    font-size: 0.82em;
    color: #3498db;
    text-decoration: none;
}
.ps-link:hover {
    text-decoration: underline;
}
.ps-score-big {
    font-size: 1.5em;
    font-weight: 800;
}
</style>
"""

st.markdown(CARD_CSS, unsafe_allow_html=True)

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
        return "#888"
    if score >= 7:
        return "#2ecc71"
    if score >= 4:
        return "#f39c12"
    return "#e74c3c"


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
        if prot > 85:
            reasons.append(f"haute teneur ({prot:.0f}%)")
        elif prot >= 80:
            reasons.append(f"bonne teneur ({prot:.0f}%)")
        elif prot >= 75:
            reasons.append(f"teneur correcte ({prot:.0f}%)")
        else:
            reasons.append(f"teneur faible ({prot:.0f}%)")

    bcaa = row.get("bcaa_per_100g_prot")
    if is_valid(bcaa):
        if bcaa > 24:
            reasons.append(f"BCAA excellent ({bcaa:.0f}g/100g prot)")
        elif bcaa >= 20:
            reasons.append(f"BCAA correct ({bcaa:.0f}g/100g prot)")
        else:
            reasons.append(f"BCAA faible ({bcaa:.0f}g/100g prot)")

    leucine = row.get("leucine_g")
    if is_valid(leucine):
        if leucine > 10:
            reasons.append(f"leucine elevee ({leucine:.1f}g)")
        elif leucine >= 8:
            reasons.append(f"leucine correcte ({leucine:.1f}g)")

    if row.get("profil_suspect"):
        reasons.append("⚠️ profil amino suspect")

    s_sante = row.get("score_sante")
    if is_valid(s_sante):
        if s_sante >= 9:
            reasons.append("composition tres propre")
        elif s_sante >= 7:
            reasons.append("composition correcte")
        elif s_sante < 5:
            reasons.append("composition a ameliorer")

    has_sucr = row.get("has_sucralose", False)
    has_ace = row.get("has_acesulfame_k", False)
    has_asp = row.get("has_aspartame", False)
    if not has_sucr and not has_ace and not has_asp:
        reasons.append("sans edulcorant")

    return " · ".join(reasons) if reasons else "Donnees insuffisantes"


def render_product_card_v2(rank, row):
    s_global = row.get("score_global")
    s_proteique = row.get("score_proteique")
    s_sante = row.get("score_sante")

    nom = html_module.escape(str(row.get("nom", "Produit inconnu") or "Produit inconnu"))
    if len(nom) > 100:
        nom = nom[:97] + "..."
    marque = html_module.escape(str(row.get("marque", "") or ""))
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

    color = score_color_10(s_global)
    stars_global, stars_num = score_to_stars_10(s_global)
    stars_prot, prot_score_num = score_to_stars_10(s_proteique)
    stars_sante, sante_num = score_to_stars_10(s_sante)

    prot_color = "#2ecc71" if is_valid(prot) and prot >= 80 else ("#f39c12" if is_valid(prot) and prot >= 70 else ("#e74c3c" if is_valid(prot) else "#888"))
    prix_kg_color = "#2ecc71" if is_valid(prix_kg) and prix_kg <= 30 else ("#f39c12" if is_valid(prix_kg) and prix_kg <= 50 else ("#e74c3c" if is_valid(prix_kg) else "#888"))

    prot_display = f"{prot:.1f}g" if is_valid(prot) else "N/A"
    prix_kg_display = f"{prix_kg:.0f}€" if is_valid(prix_kg) else "N/A"
    prix_display = f"{prix:.2f}€" if is_valid(prix) else "N/A"
    poids_display = f"{poids:.2f}kg" if is_valid(poids) else "N/A"
    bcaa_display = f"{bcaa_val:.1f}g" if is_valid(bcaa_val) else "—"
    leucine_display = f"{leucine_val:.1f}g" if is_valid(leucine_val) else "—"
    ingr_display = str(ingredient_count) if is_valid(ingredient_count) else "—"
    qual_label = quality_label(s_global)

    whey_badge = get_whey_badge(type_whey)
    origin_badge = get_origin_badge(origin_label)
    sweetener_badge = get_sweetener_badges(has_sucr, has_ace, has_asp)
    additive_badges = get_additive_badges(has_flavors, has_thick, has_color)

    extra_badges = ""
    if profil_suspect:
        extra_badges += "<span class='ps-badge ps-badge-red'>⚠️ Profil suspect</span>"
    if has_amino:
        extra_badges += "<span class='ps-badge ps-badge-green'>🧬 Aminogramme</span>"
    elif has_bcaa_flag:
        extra_badges += "<span class='ps-badge ps-badge-blue'>💊 BCAA</span>"
    if is_valid(ingredient_count) and ingredient_count > 6:
        extra_badges += f"<span class='ps-badge ps-badge-orange'>📋 {ingredient_count} ingredients</span>"

    why_text = build_why_text(row)
    link_html = f"<a href='{url}' target='_blank' class='ps-link'>🔗 Voir le produit</a>" if url else ""

    global_display = f"{s_global:.1f}/10" if is_valid(s_global) else "N/A"

    card_html = f"""
    <div class='ps-card'>
      <div style='display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start;'>

        <div style='flex-shrink:0;min-width:75px;text-align:center;'>
          <div class='ps-rank' style='color:{color};'>#{rank}</div>
          <div class='ps-stars'>{stars_global}</div>
          <div class='ps-score-big' style='color:{color};'>{global_display}</div>
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

        <div style='min-width:180px;'>
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
    st.markdown(card_html, unsafe_allow_html=True)


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
**Note Proteique /10 (60% du global)**

| Critere | Barme |
|---------|--------|
| **% proteines** (sur 5) | <70%=1, 70-75=2, 75-80=3, 80-85=4, >85%=5 |
| **BCAA/100g prot** (sur 3) | <20g=1, 20-24g=2, >24g=3 |
| **Leucine/100g prot** (sur 2) | <8g=0, 8-10g=1, >10g=2 |
| **Equilibre BCAA** | Ratio 2:1:1 attendu, malus -1 a -2 si suspect |
""")
        with leg2:
            st.markdown("""
**Note Sante /10 (40% du global)**

Commence a 10, puis malus :

| Critere | Malus |
|---------|-------|
| Edulcorant (sucralose, ace-K...) | -2 |
| Plusieurs edulcorants | -3 |
| Aromes artificiels | -1 |
| Epaississants (xanthane, carraghenane) | -1 |
| Colorants | -1 |
| Liste longue (>6 ingredients) | -1 |
""")
        with leg3:
            st.markdown("""
**Interpretation des notes**

| Note | Qualite |
|------|---------|
| 9-10 | ★★★★★ Excellent |
| 7-8 | ★★★★☆ Tres bien |
| 5-6 | ★★★☆☆ Correct |
| 3-4 | ★★☆☆☆ Moyen |
| 0-2 | ★☆☆☆☆ Faible |

**Note globale** = (proteique × 0.6) + (sante × 0.4)

*Le prix n'entre pas dans la note mais est affiche comme info*
""")

    st.divider()

    fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns([2, 1.5, 1.5, 1.5, 2])

    with fcol1:
        sort_option = st.selectbox(
            "Trier par",
            ["Note Globale", "Note Proteique", "Note Sante", "Prix/kg (croissant)", "Proteines/100g (decroissant)"],
            key="sort_option",
        )
    with fcol2:
        filter_no_sweetener = st.toggle("Sans edulcorant", key="filter_no_sweet")
    with fcol3:
        filter_france = st.toggle("Fabrication France", key="filter_france")
    with fcol4:
        filter_clean = st.toggle("Composition propre (≥8/10)", key="filter_clean")
    with fcol5:
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

    if filter_type and filter_type != "Tous":
        filtered_df = filtered_df[filtered_df["type_whey"].fillna("unknown").str.lower() == filter_type.lower()]

    sort_map = {
        "Note Globale": ("score_global", False),
        "Note Proteique": ("score_proteique", False),
        "Note Sante": ("score_sante", False),
        "Prix/kg (croissant)": ("prix_par_kg", True),
        "Proteines/100g (decroissant)": ("proteines_100g", False),
    }
    sort_col, sort_asc = sort_map.get(sort_option, ("score_global", False))

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
        "score_proteique", "score_sante", "score_global",
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
        "score_global": st.column_config.ProgressColumn("Global /10", min_value=0, max_value=10),
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


# ── AUTH PAGES ──

def page_login():
    st.title("💪 ProteinScan")
    st.markdown("##### Comparateur de proteines whey en France")
    st.markdown("---")

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

    st.markdown("---")
    st.markdown(
        "**Plan Gratuit** : 3 scans par mois | "
        "**Plan Pro** : scans illimites (bientot disponible)"
    )


# ── DASHBOARD ──

def page_dashboard():
    user = st.session_state.user

    with st.sidebar:
        st.markdown(f"### 👤 {user['display_name']}")
        st.markdown(f"📧 {user['email']}")
        plan_label = "Pro ⭐" if user["plan"] == "pro" else "Gratuit"
        st.markdown(f"📋 Plan : **{plan_label}**")

        scans_used = check_and_reset_monthly_usage(user["id"])
        scan_limit = get_scan_limit(user["plan"])
        if scan_limit is not None:
            st.markdown(f"📊 Scans ce mois : **{scans_used}/{scan_limit}**")
            st.progress(min(scans_used / scan_limit, 1.0))
        else:
            st.markdown(f"📊 Scans ce mois : **{scans_used}** (illimite)")

        st.divider()

        if st.button("Nouveau scan", type="primary", use_container_width=True):
            st.session_state.page = "scan"
            st.session_state.view_scan_id = None
            st.rerun()

        if st.button("Tableau de bord", use_container_width=True):
            st.session_state.page = "dashboard"
            st.session_state.view_scan_id = None
            st.rerun()

        st.divider()
        if st.button("Se deconnecter", use_container_width=True):
            logout()
            st.rerun()

    if st.session_state.view_scan_id:
        page_view_scan()
        return

    st.title("📊 Tableau de bord")
    st.markdown(f"Bienvenue, **{user['display_name']}** !")

    scans_used = check_and_reset_monthly_usage(user["id"])
    scan_limit = get_scan_limit(user["plan"])

    col1, col2, col3 = st.columns(3)
    with col1:
        plan_label = "Pro ⭐" if user["plan"] == "pro" else "Gratuit"
        st.metric("Plan", plan_label)
    with col2:
        if scan_limit is not None:
            st.metric("Scans restants", f"{max(0, scan_limit - scans_used)}/{scan_limit}")
        else:
            st.metric("Scans ce mois", scans_used)
    with col3:
        scans_history = get_user_scans(user["id"])
        st.metric("Total scans", len(scans_history))

    st.divider()
    st.subheader("Historique des scans")

    if not scans_history:
        st.info("Vous n'avez pas encore effectue de scan. Cliquez sur **Nouveau scan** pour commencer !")
    else:
        for scan in scans_history:
            col_date, col_count, col_action = st.columns([3, 2, 2])
            with col_date:
                created = scan["created_at"]
                if isinstance(created, str):
                    st.markdown(f"📅 {created}")
                else:
                    st.markdown(f"📅 {created.strftime('%d/%m/%Y a %H:%M')}")
            with col_count:
                st.markdown(f"📦 **{scan['product_count']}** produits")
            with col_action:
                if st.button("Voir les resultats", key=f"view_{scan['id']}"):
                    st.session_state.view_scan_id = scan["id"]
                    st.rerun()
            st.divider()

    if user["plan"] == "free":
        st.markdown("---")
        st.info(
            "**Passez au plan Pro** pour des scans illimites ! "
            "(Bientot disponible)"
        )


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

    with st.sidebar:
        st.markdown(f"### 👤 {user['display_name']}")
        plan_label = "Pro ⭐" if user["plan"] == "pro" else "Gratuit"
        st.markdown(f"📋 Plan : **{plan_label}**")

        scans_used = check_and_reset_monthly_usage(user["id"])
        scan_limit = get_scan_limit(user["plan"])
        if scan_limit is not None:
            remaining = max(0, scan_limit - scans_used)
            st.markdown(f"📊 Scans restants : **{remaining}/{scan_limit}**")
            st.progress(min(scans_used / scan_limit, 1.0))
        else:
            st.markdown(f"📊 Scans ce mois : **{scans_used}** (illimite)")

        st.divider()

        if st.button("Tableau de bord", use_container_width=True):
            st.session_state.page = "dashboard"
            st.rerun()

        st.divider()
        if st.button("Se deconnecter", use_container_width=True):
            logout()
            st.rerun()

    st.title("🔍 Nouveau scan")

    scans_used = check_and_reset_monthly_usage(user["id"])
    scan_limit = get_scan_limit(user["plan"])
    can_scan = scan_limit is None or scans_used < scan_limit

    tab_auto, tab_manual = st.tabs(["Scan automatique", "Analyse manuelle d'URLs"])

    with tab_auto:
        if not api_key:
            st.warning("La cle API Brave Search n'est pas configuree sur le serveur.")
        elif not can_scan:
            st.warning(
                f"Vous avez atteint la limite de {scan_limit} scans ce mois-ci. "
                "Passez au plan Pro pour des scans illimites !"
            )
        else:
            st.markdown(f"Lance une recherche sur **{len(SEARCH_QUERIES)} requetes** pour trouver les meilleurs produits whey du marche francais.")
            scan_button = st.button("🚀 Lancer le scan", type="primary", use_container_width=False)

            if scan_button:
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
                        if not can_user_scan(user["id"], user["plan"]):
                            status_container.warning("Limite de scans atteinte pour ce mois.")
                        else:
                            scan_id = save_scan(user["id"], products)
                            increment_scan_count(user["id"])
                            status_container.success(
                                f"✅ {len(products)} produits trouves et sauvegardes ! (Scan #{scan_id})"
                            )

                            df = pd.DataFrame(products)
                            st.divider()
                            render_results(df)
                    else:
                        status_container.warning("Aucun produit trouve.")

                except BraveAPIError as e:
                    progress_bar.empty()
                    detail_text.empty()
                    status_container.error(f"Erreur API : {e}")

    with tab_manual:
        if not can_scan:
            st.warning(
                f"Vous avez atteint la limite de {scan_limit} scans ce mois-ci."
            )
        else:
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
                        if not can_user_scan(user["id"], user["plan"]):
                            st.warning("Limite de scans atteinte pour ce mois.")
                        else:
                            scan_id = save_scan(user["id"], products)
                            increment_scan_count(user["id"])
                            st.success(
                                f"✅ {len(products)} produits extraits et sauvegardes ! (Scan #{scan_id})"
                            )

                            df = pd.DataFrame(products)
                            st.divider()
                            render_results(df)
                    else:
                        st.warning("Aucune donnee produit extraite.")


# ── ROUTER ──

if st.session_state.user is None:
    page_login()
else:
    page = st.session_state.page
    if page == "dashboard":
        page_dashboard()
    elif page == "scan":
        page_scan()
    elif page == "login":
        st.session_state.page = "dashboard"
        page_dashboard()
    else:
        page_dashboard()
