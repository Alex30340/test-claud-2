import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

from scraper import scrape_products, extract_product_data, BraveAPIError, SEARCH_QUERIES
from scoring import calculate_price_score, calculate_nutrition_score
from db import (
    init_db, create_user, get_user_by_email,
    check_and_reset_monthly_usage, increment_scan_count, get_scan_limit,
    save_scan, get_user_scans, get_scan_items, can_user_scan,
)
from auth import hash_password, verify_password

init_db()

st.set_page_config(
    page_title="ProteinScan - Comparateur Whey SaaS",
    page_icon="💪",
    layout="wide",
)

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


def score_color(score):
    if score is None or (isinstance(score, float) and pd.isna(score)):
        return "#888888"
    if score >= 70:
        return "#2ecc71"
    if score >= 40:
        return "#f39c12"
    return "#e74c3c"


def score_label(score):
    if score is None or (isinstance(score, float) and pd.isna(score)):
        return "N/A"
    return f"{score:.0f}/100"


def render_product_card(rank, row):
    s_global = row.get("score_global")
    s_prix = row.get("score_prix")
    s_nutri = row.get("score_nutrition")
    color = score_color(s_global)

    nom = row.get("nom", "Produit inconnu") or "Produit inconnu"
    if len(nom) > 80:
        nom = nom[:77] + "..."

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

        prix_valid = prix is not None and not (isinstance(prix, float) and pd.isna(prix))
        poids_valid = poids is not None and not (isinstance(poids, float) and pd.isna(poids))
        prix_kg_valid = prix_kg is not None and not (isinstance(prix_kg, float) and pd.isna(prix_kg))
        prot_valid = prot is not None and not (isinstance(prot, float) and pd.isna(prot))

        details.append(f"💰 {prix:.2f} {devise}" if prix_valid else "💰 Prix : inconnu")
        details.append(f"📦 {poids:.2f} kg" if poids_valid else "📦 Poids : inconnu")
        details.append(f"📊 {prix_kg:.2f} EUR/kg" if prix_kg_valid else "📊 Prix/kg : inconnu")
        details.append(f"🥩 {prot:.1f}g prot/100g" if prot_valid else "🥩 Proteines : inconnu")

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

        s_global_na = s_global is None or (isinstance(s_global, float) and pd.isna(s_global))
        if s_global_na:
            missing = []
            if not prix_kg_valid:
                missing.append("prix/kg")
            if not prot_valid:
                missing.append("proteines")
            st.caption(f"⚠️ Score incomplet : donnees manquantes ({', '.join(missing)})")

    st.divider()


def render_results(products_data, is_dataframe=True):
    if is_dataframe:
        df = products_data
        sorted_df = df.sort_values("score_global", ascending=False, na_position="last")
    else:
        df = pd.DataFrame(products_data)
        sorted_df = df.sort_values("score_global", ascending=False, na_position="last")

    st.subheader(f"Resultats : {len(df)} produits analyses")

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.metric("Produits trouves", len(df))
    with col_s2:
        with_score = df["score_global"].dropna().shape[0]
        st.metric("Avec score complet", f"{with_score}/{len(df)}")
    with col_s3:
        st.metric("Donnees partielles", len(df) - with_score)

    st.divider()
    st.subheader("Classement par produit")

    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        render_product_card(rank, row)

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
        df.to_excel(excel_path, index=False, sheet_name="Produits")
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

    st.title(f"Resultats du scan #{scan_id}")
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
            scan_button = st.button("Lancer le scan", type="primary", use_container_width=False)

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
                                f"{len(products)} produits trouves et sauvegardes ! (Scan #{scan_id})"
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

            analyze_button = st.button("Analyser ces URLs", type="primary", key="manual_analyze")

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
                                f"{len(products)} produits extraits et sauvegardes ! (Scan #{scan_id})"
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
