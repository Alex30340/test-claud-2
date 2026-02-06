# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
Application SaaS Python/Streamlit pour comparer les proteines whey en France. Permet aux utilisateurs de creer un compte, lancer des scans automatiques du marche via Brave Search API, voir les notes individuelles par produit (proteique /10, sante /10, globale /10), consulter l'historique, et exporter en CSV/Excel.

## Architecture

### Files
- `app.py` - Interface Streamlit SaaS (auth, dashboard, scan, historique, cartes produit V2 avec notes /10)
- `db.py` - Couche base de donnees PostgreSQL (users, scans, scan_items avec colonnes amino, additifs, scores)
- `auth.py` - Hashage et verification de mots de passe (bcrypt)
- `scraper.py` - Recherche Brave Search API + extraction multi-couche (JSON-LD, OG, microdata, HTML) + detection complete (edulcorants, aromes, epaississants, colorants, amino acids, ingredients)
- `scoring.py` - Calcul des notes proteique /10, sante /10, et globale (60/40)
- `main.py` - Script CLI (usage autonome)
- `.streamlit/config.toml` - Configuration Streamlit (port 5000)

### Database (PostgreSQL)
- `users` - Comptes utilisateurs (email, password_hash, plan, scans_this_month)
- `scans` - Historique des scans par utilisateur
- `scan_items` - Produits extraits rattaches a un scan (inclut type_whey, made_in_france, origin_label, origin_confidence, has_sucralose, has_acesulfame_k, has_aspartame, has_aminogram, mentions_bcaa, has_artificial_flavors, has_thickeners, has_colorants, ingredient_count, bcaa_per_100g_prot, leucine_g, isoleucine_g, valine_g, profil_suspect, score_proteique, score_sante, score_global, ingredients)

### Dependencies
- streamlit, httpx, beautifulsoup4, pandas, lxml, openpyxl, bcrypt, psycopg2-binary

### Required Secrets
- `BRAVE_SEARCH_API_KEY` - Cle API Brave Search (commence par BSA...)
- Note: `BRAVE_API_KEY` est aussi verifie comme fallback

### Plans utilisateurs
- **Free** : 3 scans par mois
- **Pro** : illimite (Stripe non encore integre)

### Scoring System (V2 - Notes sur 10)

#### Note Proteique /10 (60% du global)
- Critere 1 : % proteines (sur 5 pts) : <70%=1, 70-75=2, 75-80=3, 80-85=4, >85%=5
- Critere 2 : BCAA pour 100g de proteines (sur 3 pts) : <20g=1, 20-24g=2, >24g=3
- Critere 3 : Leucine pour 100g de proteines (sur 2 pts) : <8g=0, 8-10g=1, >10g=2
- Critere 4 : Equilibre BCAA (controle qualite) : ratio 2:1:1 attendu (~10g leucine, ~5g iso, ~5g val), malus -1 a -2 si ratio suspect

#### Note Sante /10 (40% du global)
Commence a 10, puis malus :
- Edulcorant artificiel (sucralose, acesulfame-K) : -2
- Plusieurs edulcorants : -3 (au lieu de -2)
- Aromes artificiels : -1
- Epaississants (gomme xanthane, carraghenanes) : -1
- Colorants : -1
- Liste d'ingredients longue (>6 ingredients) : -1

#### Note Globale
- Note globale = (note proteique × 0.6) + (note sante × 0.4)
- Le prix n'entre PAS dans la note globale mais est affiche comme information

#### Interpretation
- 9-10 : Excellent (tres propre)
- 7-8 : Tres bien (correcte)
- 5-6 : Correct (moyenne)
- 3-4 : Moyen
- <3 : Faible (peu recommandee)

### Detection Features
- Detection du type de whey : Native, Isolate, Hydrolysate, Concentrate
- Detection fabrication francaise : patterns "fabrique en France", "Made in France", "lait francais"
- Detection origine : France (confidence 0.9), EU (confidence 0.7), Inconnu (confidence 0.3)
- Detection edulcorants : Sucralose, Acesulfame-K (E950), Aspartame (E951)
- Detection aromes artificiels : patterns "aromes artificiels", "aromes synthetiques"
- Detection epaississants : gomme xanthane, carraghenane, gomme guar, E407, E415, E412
- Detection colorants : E1xx, dioxyde de titane, beta-carotene
- Detection aminogramme : profil acides amines complet (leucine + isoleucine + valine)
- Detection BCAA : mentions "BCAA" ou ratio "2:1:1"
- Extraction quantites amino : leucine, isoleucine, valine (g ou mg), calcul BCAA/100g prot
- Comptage ingredients depuis bloc ingredients
- Extraction bloc ingredients depuis page HTML

### UI V2 Features
- Notes sur /10 avec etoiles (★★★★☆ sur 5 etoiles, echelle /10)
- Badges visuels : type whey, origine, edulcorants, aromes, epaississants, colorants, profil suspect, nb ingredients
- Ligne "Pourquoi ce score ?" expliquant le classement
- Metriques BCAA/100g prot et Leucine/100g prot affichees sur chaque carte
- Sous-scores : Note Proteique et Note Sante avec etoiles separees
- Barre de filtres : tri multi-critere, toggles (sans edulcorant, France, composition propre ≥8/10), dropdown type whey, recherche texte
- Legende/bareme repliable (accordion) avec tableaux de notation detailles
- Tableau complet avec barres de progression /10

### Data Extraction Strategy
Multi-couche pour maximiser la couverture :
1. JSON-LD (schema.org Product)
2. Open Graph meta tags (og:title, product:price:amount)
3. Schema.org microdata (itemprop attributes)
4. HTML regex fallbacks (price classes, text patterns)
5. Bloc ingredients pour detection edulcorants et additifs
6. Extraction amino acids (leucine, isoleucine, valine, BCAA) depuis texte page
Sites exclus : Amazon (503), Decathlon (403), reseaux sociaux

### Performance
- Extraction parallele via ThreadPoolExecutor (8 workers simultanes)
- REQUEST_DELAY = 1.1s entre requetes Brave API
- HTTP_TIMEOUT = 12s par site
- Le scan automatique et l'analyse manuelle utilisent tous deux l'extraction parallele

### Running
- Streamlit UI: `streamlit run app.py --server.port 5000`
- CLI: `python main.py`

### Notes
- Stripe integration was proposed but user dismissed the setup. Can be added later.
- Database is auto-initialized on app startup via init_db() with automatic migration for new columns.
- Future vision: whey is just one tab; the platform will include profile analysis, muscle programs, diet programs, and recommendations based on user profile (height, weight, body analysis).
- Score prix calcule_price_score() est conserve pour affichage informatif mais ne fait plus partie de la note globale.
