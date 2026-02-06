# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
Application SaaS Python/Streamlit pour comparer les proteines whey en France. Permet aux utilisateurs de creer un compte, lancer des scans automatiques du marche via Brave Search API, voir les scores individuels par produit (sante, prix, nutrition), consulter l'historique, et exporter en CSV/Excel.

## Architecture

### Files
- `app.py` - Interface Streamlit SaaS (auth, dashboard, scan, historique, cartes produit avec scores sante)
- `db.py` - Couche base de donnees PostgreSQL (users, scans, scan_items avec colonnes sante)
- `auth.py` - Hashage et verification de mots de passe (bcrypt)
- `scraper.py` - Recherche Brave Search API + extraction multi-couche (JSON-LD, OG, microdata, HTML) + detection sante (edulcorants, type whey, origine France, aminogramme)
- `scoring.py` - Calcul des scores prix/valeur, nutrition, sante, et global
- `main.py` - Script CLI (usage autonome)
- `.streamlit/config.toml` - Configuration Streamlit (port 5000)

### Database (PostgreSQL)
- `users` - Comptes utilisateurs (email, password_hash, plan, scans_this_month)
- `scans` - Historique des scans par utilisateur
- `scan_items` - Produits extraits rattaches a un scan (inclut type_whey, made_in_france, has_sucralose, has_acesulfame_k, has_aspartame, has_aminogram, mentions_bcaa, score_sante)

### Dependencies
- streamlit, httpx, beautifulsoup4, pandas, lxml, openpyxl, bcrypt, psycopg2-binary

### Required Secrets
- `BRAVE_SEARCH_API_KEY` - Cle API Brave Search (commence par BSA...)
- Note: `BRAVE_API_KEY` est aussi verifie comme fallback

### Plans utilisateurs
- **Free** : 3 scans par mois
- **Pro** : illimite (Stripe non encore integre, l'utilisateur a decline la configuration)

### Scoring System
- **Score Sante (55% du global)** : Base 50, modifie par type whey (Native +18, Isolate +14, Hydrolysate +12, Concentrate -8), fabrication France (+8), aminogramme (+8) ou BCAA (+3), edulcorants (Sucralose -10, Acesulfame-K -8, Aspartame -18, max -22), proteines hautes (+6) ou basses (-6)
- **Score Prix (25% du global)** : 20 EUR/kg = 100, 80 EUR/kg = 0 (interpolation lineaire)
- **Score Nutrition (20% du global)** : 90g prot/100g = 100, 60g prot/100g = 0

### Health Detection Features
- Detection du type de whey : Native, Isolate, Hydrolysate, Concentrate
- Detection fabrication francaise : patterns "fabrique en France", "Made in France", "lait francais"
- Detection edulcorants : Sucralose, Acesulfame-K (E950), Aspartame (E951)
- Detection aminogramme : profil acides amines complet (leucine + isoleucine + valine)
- Detection BCAA : mentions "BCAA" ou ratio "2:1:1"

### Data Extraction Strategy
Multi-couche pour maximiser la couverture :
1. JSON-LD (schema.org Product)
2. Open Graph meta tags (og:title, product:price:amount)
3. Schema.org microdata (itemprop attributes)
4. HTML regex fallbacks (price classes, text patterns)
5. Bloc ingredients pour detection edulcorants
Sites exclus : Amazon (503), Decathlon (403), reseaux sociaux

### Performance
- Extraction parallele via ThreadPoolExecutor (8 workers simultanes)
- REQUEST_DELAY = 0.3s entre requetes Brave API
- HTTP_TIMEOUT = 8s par site
- Le scan automatique et l'analyse manuelle utilisent tous deux l'extraction parallele

### Running
- Streamlit UI: `streamlit run app.py --server.port 5000`
- CLI: `python main.py`

### Notes
- Stripe integration was proposed but user dismissed the setup. Can be added later.
- Database is auto-initialized on app startup via init_db() with automatic migration for new columns.
- Future vision: whey is just one tab; the platform will include profile analysis, muscle programs, diet programs, and recommendations based on user profile (height, weight, body analysis).
