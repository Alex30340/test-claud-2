# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
Application SaaS Python/Streamlit pour comparer les proteines whey en France. Permet aux utilisateurs de creer un compte, lancer des scans automatiques du marche via Brave Search API, voir les scores individuels par produit, consulter l'historique, et exporter en CSV/Excel.

## Architecture

### Files
- `app.py` - Interface Streamlit SaaS (auth, dashboard, scan, historique)
- `db.py` - Couche base de donnees PostgreSQL (users, scans, scan_items)
- `auth.py` - Hashage et verification de mots de passe (bcrypt)
- `scraper.py` - Recherche Brave Search API + extraction multi-couche (JSON-LD, OG, microdata, HTML)
- `scoring.py` - Calcul des scores prix/valeur, nutrition, et global
- `main.py` - Script CLI (usage autonome)
- `.streamlit/config.toml` - Configuration Streamlit (port 5000)

### Database (PostgreSQL)
- `users` - Comptes utilisateurs (email, password_hash, plan, scans_this_month)
- `scans` - Historique des scans par utilisateur
- `scan_items` - Produits extraits rattaches a un scan

### Dependencies
- streamlit, httpx, beautifulsoup4, pandas, lxml, openpyxl, bcrypt, psycopg2-binary

### Required Secrets
- `BRAVE_SEARCH_API_KEY` - Cle API Brave Search (commence par BSA...)
- Note: `BRAVE_API_KEY` est aussi verifie comme fallback

### Plans utilisateurs
- **Free** : 3 scans par mois
- **Pro** : illimite (Stripe non encore integre, l'utilisateur a decline la configuration)

### Scoring System
- Score Prix: 20 EUR/kg = 100, 80 EUR/kg = 0 (interpolation lineaire)
- Score Nutrition: 90g prot/100g = 100, 60g prot/100g = 0
- Score Global: 50% prix + 50% nutrition

### Data Extraction Strategy
Multi-couche pour maximiser la couverture :
1. JSON-LD (schema.org Product)
2. Open Graph meta tags (og:title, product:price:amount)
3. Schema.org microdata (itemprop attributes)
4. HTML regex fallbacks (price classes, text patterns)
Sites exclus : Amazon (503), Decathlon (403), reseaux sociaux

### Running
- Streamlit UI: `streamlit run app.py --server.port 5000`
- CLI: `python main.py`

### Notes
- Stripe integration was proposed but user dismissed the setup. Can be added later.
- Database is auto-initialized on app startup via init_db()
