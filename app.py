import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

from scraper import scrape_products, extract_product_data, BraveAPIError, SEARCH_QUERIES
from scoring import calculate_price_score, calculate_nutrition_score

st.set_page_config(
    page_title="Comparateur Proteines Whey",
    page_icon="💪",
    layout="wide",
)

st.title("Comparateur de Proteines en Poudre")
st.markdown("Analyse automatique du marche des proteines whey en France")

api_key = os.environ.get("BRAVE_API_KEY", "") or os.environ.get("BRAVE_SEARCH_API_KEY", "")

with st.sidebar:
    st.header("Configuration")

    if not api_key:
        api_key_input = st.text_input(
            "Cle API Brave Search",
            type="password",
            help="Obtenez une cle gratuite sur https://brave.com/search/api/",
        )
        if api_key_input:
            api_key = api_key_input
    else:
        st.success("Cle API configuree")

    st.divider()
    st.subheader("Mots-cles de recherche")
    for q in SEARCH_QUERIES:
        st.markdown(f"- {q}")

    st.divider()
    st.subheader("Bareme de scores")
    st.markdown("""
    **Score Prix/Valeur :**
    - 20 EUR/kg = 100 pts
    - 80 EUR/kg = 0 pts

    **Score Nutrition :**
    - 90g prot/100g = 100 pts
    - 60g prot/100g = 0 pts

    **Score Global :**
    50% prix + 50% nutrition
    """)

if "products_df" not in st.session_state:
    st.session_state.products_df = None
if "scan_done" not in st.session_state:
    st.session_state.scan_done = False

tab_auto, tab_manual = st.tabs(["Scan automatique", "Analyse manuelle d'URLs"])

with tab_auto:
    scan_button = st.button(
        "Lancer le scan",
        type="primary",
        disabled=not api_key,
        use_container_width=False,
    )

    if not api_key:
        st.warning("Veuillez configurer votre cle API Brave Search pour utiliser le scan automatique.")
        st.markdown("""
        ### Comment obtenir une cle API Brave Search ?
        1. Rendez-vous sur [api-dashboard.search.brave.com](https://api-dashboard.search.brave.com)
        2. Creez un compte gratuit
        3. Allez dans **API Keys** et creez une cle (plan Free = 2000 requetes/mois)
        4. La cle commence par **BSA...**
        5. Ajoutez-la comme secret `BRAVE_SEARCH_API_KEY` dans Replit

        Vous pouvez aussi utiliser l'onglet **Analyse manuelle d'URLs** pour tester l'extraction.
        """)

    if scan_button and api_key:
        status_container = st.empty()
        progress_bar = st.progress(0)
        detail_text = st.empty()

        def update_progress(current, total, detail=""):
            if total > 0:
                progress_bar.progress(current / total)
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
                df = pd.DataFrame(products)
                st.session_state.products_df = df
                st.session_state.scan_done = True
                status_container.success(f"{len(products)} produits trouves et analyses !")
            else:
                status_container.warning("Aucun produit trouve. Les pages visitees ne contenaient pas de donnees produit exploitables.")
                st.session_state.scan_done = False

        except BraveAPIError as e:
            progress_bar.empty()
            detail_text.empty()
            error_msg = str(e)
            if "SUBSCRIPTION_TOKEN_INVALID" in error_msg:
                status_container.error(
                    "La cle API Brave Search est invalide. "
                    "Verifiez que vous avez copie la bonne cle depuis "
                    "[api-dashboard.search.brave.com/app/keys](https://api-dashboard.search.brave.com/app/keys). "
                    "La cle doit commencer par 'BSA...'."
                )
            else:
                status_container.error(f"Erreur API Brave Search : {error_msg}")

with tab_manual:
    st.markdown("Entrez des URLs de pages produit (une par ligne) pour extraire les donnees :")
    urls_input = st.text_area(
        "URLs de pages produit",
        placeholder="https://www.myprotein.fr/sports-nutrition/impact-whey-protein/10530943.html\nhttps://www.bulk.com/fr/whey-protein.html",
        height=150,
    )

    analyze_button = st.button(
        "Analyser ces URLs",
        type="primary",
        key="manual_analyze",
    )

    if analyze_button and urls_input.strip():
        urls = [u.strip() for u in urls_input.strip().split("\n") if u.strip().startswith("http")]

        if not urls:
            st.warning("Aucune URL valide trouvee. Les URLs doivent commencer par http:// ou https://")
        else:
            progress_bar = st.progress(0)
            status_text = st.empty()
            products = []

            for i, url in enumerate(urls):
                status_text.text(f"Extraction {i+1}/{len(urls)} : {url[:80]}...")
                progress_bar.progress((i + 1) / len(urls))
                product = extract_product_data(url)
                if product:
                    products.append(product)
                elif i < len(urls) - 1:
                    time.sleep(1)

            progress_bar.empty()
            status_text.empty()

            if products:
                df = pd.DataFrame(products)
                st.session_state.products_df = df
                st.session_state.scan_done = True
                st.success(f"{len(products)} produits extraits sur {len(urls)} URLs analysees !")
            else:
                st.warning(
                    "Aucune donnee produit extraite. "
                    "Les pages ne contiennent peut-etre pas de donnees structurees exploitables."
                )


def score_color(score):
    if score is None or pd.isna(score):
        return "#888888"
    if score >= 70:
        return "#2ecc71"
    if score >= 40:
        return "#f39c12"
    return "#e74c3c"


def score_label(score):
    if score is None or pd.isna(score):
        return "N/A"
    return f"{score:.0f}/100"


def data_status_icon(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "⚠️"
    return ""


if st.session_state.products_df is not None and st.session_state.scan_done:
    df = st.session_state.products_df

    st.divider()
    st.subheader(f"Resultats : {len(df)} produits analyses")

    col_summary1, col_summary2, col_summary3 = st.columns(3)
    with col_summary1:
        st.metric("Produits trouves", len(df))
    with col_summary2:
        with_score = df["score_global"].dropna().shape[0]
        st.metric("Avec score complet", f"{with_score}/{len(df)}")
    with col_summary3:
        without_data = len(df) - with_score
        st.metric("Donnees partielles", without_data)

    st.divider()
    st.subheader("Classement par produit")

    sorted_df = df.sort_values("score_global", ascending=False, na_position="last")

    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        s_global = row.get("score_global")
        s_prix = row.get("score_prix")
        s_nutri = row.get("score_nutrition")
        color = score_color(s_global)

        nom = row.get("nom", "Produit inconnu")
        if len(nom) > 80:
            nom = nom[:77] + "..."

        with st.container():
            col_rank, col_info, col_scores = st.columns([0.8, 4, 3])

            with col_rank:
                st.markdown(
                    f"<div style='text-align:center;padding-top:10px;'>"
                    f"<span style='font-size:2em;font-weight:bold;color:{color};'>#{rank}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            with col_info:
                marque = row.get("marque", "")
                marque_text = f" — {marque}" if marque else ""
                st.markdown(f"**{nom}**{marque_text}")

                details = []
                prix = row.get("prix")
                devise = row.get("devise", "EUR")
                poids = row.get("poids_kg")
                prix_kg = row.get("prix_par_kg")
                prot = row.get("proteines_100g")

                if pd.notna(prix):
                    details.append(f"💰 {prix:.2f} {devise}")
                else:
                    details.append("💰 Prix : inconnu")

                if pd.notna(poids):
                    details.append(f"📦 {poids:.2f} kg")
                else:
                    details.append("📦 Poids : inconnu")

                if pd.notna(prix_kg):
                    details.append(f"📊 {prix_kg:.2f} EUR/kg")
                else:
                    details.append("📊 Prix/kg : inconnu")

                if pd.notna(prot):
                    details.append(f"🥩 {prot:.1f}g prot/100g")
                else:
                    details.append("🥩 Proteines : inconnu")

                st.markdown(" | ".join(details))

                url = row.get("url", "")
                if url:
                    st.markdown(f"[Voir le produit]({url})")

            with col_scores:
                sc1, sc2, sc3 = st.columns(3)
                with sc1:
                    st.markdown(
                        f"<div style='text-align:center;'>"
                        f"<div style='font-size:0.8em;color:#666;'>Prix</div>"
                        f"<div style='font-size:1.4em;font-weight:bold;color:{score_color(s_prix)};'>{score_label(s_prix)}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with sc2:
                    st.markdown(
                        f"<div style='text-align:center;'>"
                        f"<div style='font-size:0.8em;color:#666;'>Nutrition</div>"
                        f"<div style='font-size:1.4em;font-weight:bold;color:{score_color(s_nutri)};'>{score_label(s_nutri)}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with sc3:
                    st.markdown(
                        f"<div style='text-align:center;'>"
                        f"<div style='font-size:0.8em;color:#666;'>Global</div>"
                        f"<div style='font-size:1.8em;font-weight:bold;color:{color};'>{score_label(s_global)}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                if pd.isna(s_global):
                    missing = []
                    if pd.isna(prix_kg):
                        missing.append("prix/kg")
                    if pd.isna(prot):
                        missing.append("proteines")
                    st.caption(f"⚠️ Score incomplet : donnees manquantes ({', '.join(missing)})")

            st.divider()

    st.subheader("Tableau complet")

    display_cols = [
        "nom", "marque", "prix", "devise", "poids_kg",
        "prix_par_kg", "proteines_100g", "score_prix",
        "score_nutrition", "score_global", "disponibilite",
    ]
    existing_cols = [c for c in display_cols if c in df.columns]

    column_config = {
        "nom": st.column_config.TextColumn("Produit", width="large"),
        "marque": st.column_config.TextColumn("Marque"),
        "prix": st.column_config.NumberColumn("Prix", format="%.2f"),
        "devise": st.column_config.TextColumn("Devise"),
        "poids_kg": st.column_config.NumberColumn("Poids (kg)", format="%.2f"),
        "prix_par_kg": st.column_config.NumberColumn("Prix/kg", format="%.2f"),
        "proteines_100g": st.column_config.NumberColumn("Prot/100g", format="%.1f"),
        "score_prix": st.column_config.ProgressColumn("Score Prix", min_value=0, max_value=100),
        "score_nutrition": st.column_config.ProgressColumn("Score Nutrition", min_value=0, max_value=100),
        "score_global": st.column_config.ProgressColumn("Score Global", min_value=0, max_value=100),
        "disponibilite": st.column_config.TextColumn("Dispo"),
    }

    st.dataframe(
        sorted_df[existing_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("Exporter les donnees")

    col_e1, col_e2 = st.columns(2)

    csv_data = df.to_csv(index=False, sep=";", encoding="utf-8-sig")
    csv_path = "market_snapshot.csv"
    df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

    with col_e1:
        st.download_button(
            label="Telecharger CSV",
            data=csv_data,
            file_name=f"proteines_whey_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col_e2:
        excel_path = "market_snapshot.xlsx"
        df.to_excel(excel_path, index=False, sheet_name="Produits")
        with open(excel_path, "rb") as f:
            st.download_button(
                label="Telecharger Excel",
                data=f.read(),
                file_name=f"proteines_whey_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
