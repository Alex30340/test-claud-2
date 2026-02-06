import streamlit as st
import pandas as pd
import os
from datetime import datetime

from scraper import scrape_products, SEARCH_QUERIES
from scoring import calculate_price_score, calculate_nutrition_score

st.set_page_config(
    page_title="Comparateur Protéines Whey",
    page_icon="💪",
    layout="wide",
)

st.title("Comparateur de Protéines en Poudre")
st.markdown("Analyse automatique du marché des protéines whey en France")

api_key = os.environ.get("BRAVE_SEARCH_API_KEY", "")

with st.sidebar:
    st.header("Configuration")

    if not api_key:
        api_key_input = st.text_input(
            "Clé API Brave Search",
            type="password",
            help="Obtenez une clé gratuite sur https://brave.com/search/api/",
        )
        if api_key_input:
            api_key = api_key_input
    else:
        st.success("Clé API configurée")

    st.divider()
    st.subheader("Mots-clés de recherche")
    for q in SEARCH_QUERIES:
        st.markdown(f"- {q}")

    st.divider()
    st.subheader("Barème de scores")
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

col1, col2 = st.columns([1, 3])

with col1:
    scan_button = st.button(
        "Lancer le scan",
        type="primary",
        disabled=not api_key,
        use_container_width=True,
    )

if not api_key:
    st.warning("Veuillez configurer votre clé API Brave Search dans la barre latérale pour commencer.")
    st.markdown("""
    ### Comment obtenir une clé API Brave Search ?
    1. Rendez-vous sur [brave.com/search/api](https://brave.com/search/api/)
    2. Créez un compte gratuit
    3. Copiez votre clé API
    4. Collez-la dans le champ ci-dessus ou ajoutez-la comme secret `BRAVE_SEARCH_API_KEY`
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
        status_container.success(f"{len(products)} produits trouvés et analysés !")
    else:
        status_container.warning("Aucun produit trouvé. Vérifiez votre clé API ou réessayez.")
        st.session_state.scan_done = False

if st.session_state.products_df is not None and st.session_state.scan_done:
    df = st.session_state.products_df

    st.divider()

    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    with col_m1:
        st.metric("Produits trouvés", len(df))
    with col_m2:
        avg_score = df["score_global"].dropna().mean()
        st.metric("Score moyen", f"{avg_score:.1f}" if pd.notna(avg_score) else "N/A")
    with col_m3:
        avg_price = df["prix_par_kg"].dropna().mean()
        st.metric("Prix moyen /kg", f"{avg_price:.2f} EUR" if pd.notna(avg_price) else "N/A")
    with col_m4:
        avg_prot = df["proteines_100g"].dropna().mean()
        st.metric("Protéines moy. /100g", f"{avg_prot:.1f}g" if pd.notna(avg_prot) else "N/A")

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
        st.markdown(f"- **{row['nom'][:80]}** — {price_text} — {score_text} — [Voir le produit]({row['url']})")

    st.divider()
    st.subheader("Exporter les données")

    col_e1, col_e2 = st.columns(2)

    csv_data = df.to_csv(index=False, sep=";", encoding="utf-8-sig")
    csv_path = "market_snapshot.csv"
    df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

    with col_e1:
        st.download_button(
            label="Télécharger CSV",
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
                label="Télécharger Excel",
                data=f.read(),
                file_name=f"proteines_whey_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
