import streamlit as st
import pandas as pd
import os
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

api_key = os.environ.get("BRAVE_SEARCH_API_KEY", "")

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
                status_container.warning("Aucun produit trouve. Les pages visitees ne contenaient pas de donnees produit structurees (JSON-LD).")
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

            import time
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
                    "Les pages ne contiennent peut-etre pas de donnees structurees JSON-LD (schema.org Product)."
                )

if st.session_state.products_df is not None and st.session_state.scan_done:
    df = st.session_state.products_df

    st.divider()

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    with col_m1:
        st.metric("Produits trouves", len(df))
    with col_m2:
        avg_score = df["score_global"].dropna().mean()
        st.metric("Score moyen", f"{avg_score:.1f}" if pd.notna(avg_score) else "N/A")
    with col_m3:
        avg_price = df["prix_par_kg"].dropna().mean()
        st.metric("Prix moyen /kg", f"{avg_price:.2f} EUR" if pd.notna(avg_price) else "N/A")
    with col_m4:
        avg_prot = df["proteines_100g"].dropna().mean()
        st.metric("Proteines moy. /100g", f"{avg_prot:.1f}g" if pd.notna(avg_prot) else "N/A")

    st.divider()
    st.subheader("Classement des produits")

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

    sorted_df = df.sort_values("score_global", ascending=False, na_position="last")

    st.dataframe(
        sorted_df[existing_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.subheader("Liens vers les produits")
    for _, row in sorted_df.iterrows():
        score_text = f"Score: {row['score_global']:.0f}" if pd.notna(row.get("score_global")) else "Score: N/A"
        price_text = f"{row['prix']:.2f} {row.get('devise', 'EUR')}" if pd.notna(row.get("prix")) else "Prix N/A"
        st.markdown(f"- **{row['nom'][:80]}** -- {price_text} -- {score_text} -- [Voir le produit]({row['url']})")

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
