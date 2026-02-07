# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
Application SaaS Python/Streamlit pour comparer les proteines whey en France. Moteur produit avec separation Product/Offer, pipelines Discovery (Brave Search) et Refresh (re-scrape prix), scoring /10 (proteique + sante), catalogue avec score de confiance, et interface admin.

## Architecture

### Files
- `app.py` - Interface Streamlit SaaS (auth, dashboard, catalogue, scan, admin, cartes produit V2)
- `db.py` - Couche base de donnees PostgreSQL (users, scans, scan_items, products, offers, pipeline_runs)
- `auth.py` - Hashage et verification de mots de passe (bcrypt)
- `extractor.py` - Extraction prix 4 niveaux (JSON-LD, OpenGraph, Next/Nuxt, regex), currency, poids, detection needs_js_render
- `validator.py` - Validation prix/poids, compute_confidence_v2 avec support needs_js_render
- `page_validator.py` - Validateur strict de page produit (is_bad_url, is_product_page, extract_jsonld_product_offer, has_add_to_cart_signals, has_price_signals, has_weight_signals, validate_url_debug)
- `scraper.py` - Recherche Brave Search API + extraction multi-couche + pipelines Discovery/Refresh + confidence scoring
- `scoring.py` - Calcul des notes proteique /10, sante /10, prix /10, et note finale (50/35/15 + bonus premium)
- `test_extractor.py` - Tests unitaires extracteur (15 cas : JSON-LD, OG, Next.js, regex, needs_js_render, crossed prices)
- `main.py` - Script CLI (usage autonome)
- `.streamlit/config.toml` - Configuration Streamlit (port 5000)

### Database (PostgreSQL)
- `users` - Comptes utilisateurs (email, password_hash, plan, scans_this_month)
- `scans` - Historique des scans par utilisateur (legacy, toujours fonctionnel)
- `scan_items` - Produits extraits rattaches a un scan (legacy)
- `products` - Catalogue stable (name, brand, type_whey, nutrition, ingredients, scores, origin, normalized_key pour dedup)
- `offers` - Offres marchands volatiles (product_id FK, merchant, url, prix, poids_kg, prix_par_kg, confidence, fail_count, is_active, discovery_source, needs_js_render, price_source)
- `pipeline_runs` - Historique des executions Discovery/Refresh (run_type, status, counts, timestamps)

### Product/Offer Architecture
- **Product** = donnees stables (ingredients, scores, type whey, origine, flags edulcorants/additifs)
- **Offer** = donnees volatiles (prix, disponibilite, marchand, URL, poids, confidence)
- Un Product peut avoir plusieurs Offers (meme produit chez differents marchands)
- Deduplication par `normalized_key` (brand + name normalise, sans poids)

### Pipelines
- **Discovery** (hebdomadaire) : Multi-requetes Brave Search (par type whey, par marque seed, long-tail) -> max_per_domain filtering -> extraction parallele -> Product + Offer avec confidence + discovery_source -> filtre confidence < 0.2
  - Parametres configurables : max_per_domain (defaut 2), use_brand_seeds, block_domains, scrape_limit (defaut 200)
  - Retourne stats : domains_found, brands_found, brands_missing
- **Refresh** (quotidien) : re-scrape des Offers actives (confidence >= 0.3) -> mise a jour prix/dispo -> mark failed apres 3 echecs

### Discovery Strategy
- **SEED_BRANDS** : 28 marques FR/EU avec domaines connus (Novoma, Nutrimuscle, Nutri&Co, Eiyolab, Greenwhey, Nutripure, Foodspring, etc.)
- **Multi-requetes** : par type whey (isolate, native, concentree, hydrolysee) x intent keywords ("ajouter au panier", "en stock", "acheter", "prix")
- **Brand seeds** : requetes automatiques "whey {brand} acheter site:.fr" + "site:{domain} whey" pour chaque marque
- **Long-tail** : requetes generiques orientees ecommerce avec exclusion des gros domaines
- **MAX_PER_DOMAIN** = 2 : limite le nombre d'URLs par domaine par run pour diversifier
- **BLOCK_DOMAINS** : liste de domaines a exclure (myprotein, bulk, amazon, decathlon)
- **SEARCH_EXCLUSIONS** : -blog -forum -comparatif -guide -test -avis -pdf -category -collections -search
- **Discovery Health** : section admin montrant domaines uniques, marques trouvees/manquantes, top domaines

### Confidence Scoring (0-1)
Score composite base sur :
- Presence JSON-LD Product (0.9 vs 0.4)
- Prix extrait (0.8 vs 0.1)
- Poids extrait (0.7 vs 0.2)
- Proteines extraites (0.8 vs 0.2)
- Prix/kg dans fourchette 10-100 EUR/kg (0.9 vs 0.2-0.5)
- Nom produit > 10 chars (0.6 vs 0.3)

### Dependencies
- streamlit, httpx, beautifulsoup4, pandas, lxml, openpyxl, bcrypt, psycopg2-binary

### Required Secrets
- `BRAVE_SEARCH_API_KEY` - Cle API Brave Search (commence par BSA...)
- Note: `BRAVE_API_KEY` est aussi verifie comme fallback

### Plans utilisateurs
- **Free** : 3 scans par mois
- **Pro** : illimite (Stripe non encore integre)

### Scoring System (V3 - Note Finale /10)

#### Note Proteique /10 (50% de la finale)
- Critere 1 : % proteines (sur 5 pts) : <70%=1, 70-75=2, 75-80=3, 80-85=4, >85%=5
- Critere 2 : BCAA pour 100g de proteines (sur 3 pts) : <20g=1, 20-24g=2, >24g=3 (neutre si absent)
- Critere 3 : Leucine pour 100g de proteines (sur 2 pts) : <8g=0, 8-10g=1, >10g=2 (neutre si absent)
- Critere 4 : Equilibre BCAA (controle qualite) : ratio 2:1:1 attendu (~10g leucine, ~5g iso, ~5g val), malus -1 a -2 si ratio suspect

#### Note Sante /10 (35% de la finale)
Commence a 10, puis malus :
- Edulcorant artificiel (sucralose, acesulfame-K) : -2
- Plusieurs edulcorants : -3 (au lieu de -2)
- Aromes artificiels : -1
- Epaississants (gomme xanthane, carraghenanes) : -1
- Colorants : -1
- 7-9 ingredients : -0.5
- 10-14 ingredients : -1.0
- 15-20 ingredients : -2.0
- >20 ingredients : -3.0

#### Note Prix /10 (15% de la finale)
- 12 paliers de <=15 EUR/kg (10/10) a >160 EUR/kg (0/10)
- Score par paliers fixes (stepwise)

#### Note Finale /10
- note_finale = (proteique x 0.50) + (sante x 0.35) + (prix x 0.15) + bonus_premium
- Bonus premium (max +1.3) :
  - Proteines >= 90g/100g : +0.5
  - Leucine >= 10.5g : +0.3
  - Aminogramme present : +0.3
  - Origine France : +0.2
- Plafond a 10.0
- score_global legacy (60/40) conserve pour compatibilite

#### Badges
- 🏅 TOP QUALITE : score_proteique >= 8.5 AND score_sante >= 8.5 AND ingredient_count <= 9
- 🔍 Transparence faible : BCAA ou leucine non trouves

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

### UI Pages
- **Login/Signup** : authentification utilisateur
- **Dashboard** : tableau de bord avec historique des scans
- **Catalogue** : vue du catalogue produit (Product/Offer), filtres, tri, badges confiance
- **Nouveau scan** : scan automatique (Brave) ou analyse manuelle d'URLs
- **Admin** : lancer Discovery/Refresh, voir historique pipelines, stats catalogue
- **Vue scan** : detail d'un scan passe

### UI V2 Features
- Notes sur /10 avec etoiles (echelle /10 -> 5 etoiles)
- Badges visuels : type whey, origine, edulcorants, aromes, epaississants, colorants, profil suspect, nb ingredients, confiance
- Badge confiance colore : vert >= 70%, jaune >= 40%, rouge < 40%
- Ligne "Pourquoi ce score ?" expliquant le classement
- Metriques BCAA/100g prot et Leucine/100g prot affichees sur chaque carte
- Sous-scores : Note Proteique et Note Sante avec etoiles separees
- Barre de filtres : tri multi-critere, toggles (sans edulcorant, France, composition propre >= 8/10), dropdown type whey, recherche texte
- Legende/bareme repliable (accordion) avec tableaux de notation detailles
- Tableau complet avec barres de progression /10
- Export CSV/Excel

### Data Extraction Strategy
Multi-couche pour maximiser la couverture :
1. JSON-LD (schema.org Product) - highest priority for price and product data
   - offers.price, offers.priceSpecification.price, offers.lowPrice
2. Open Graph meta tags (product:price:amount, og:price:amount, twitter:data1)
3. Scripts JSON (Next.js __NEXT_DATA__, Nuxt __NUXT__, window.__INITIAL_STATE__)
   - Walk JSON recursively pour trouver "price"
4. Regex fallback sur HTML :
   - Classes CSS prioritaires (current-price, sale-price, product-price)
   - Proximite panier ("ajouter au panier") avec pattern prix "xx,xx €"
   - Exclusion prix barres (<del>, <s>, class old/was/crossed/barre)
   - Exclusion contextes parasites (abonnement, livraison, a partir de)
5. Bloc ingredients pour detection edulcorants et additifs
6. Extraction amino acids (leucine, isoleucine, valine, BCAA) depuis texte page

### JavaScript Render Detection
- Si aucun prix detecte MAIS presence Next.js/Nuxt/React OU boutons panier => needs_js_render=True
- needs_js_render stocke dans offers
- Si needs_js_render=True et pas de prix => confidence plafonnee a 0.3
- Catalogue filtre par defaut a confidence >= 0.75 (exclut pages JS sans prix)

### Product Page Validation (Strict - page_validator.py)
- **is_bad_url(url)** : pre-filtre URL avant scraping (blogs, forums, guides, categories, PDF, domaines bloques)
- **is_product_page(url, html)** : validation stricte apres scraping, retourne (bool, reasons dict)
- **Regles d'acceptation** :
  - (JSON-LD Product + Offer avec price ou availability) => accepte
  - (signaux panier + signaux prix + signaux poids) => accepte
  - (signaux panier + signaux prix) => accepte (fallback sans poids)
- **Regles de rejet** :
  - is_bad_url() True => rejet immediat
  - H1/title contient "comparatif/guide/top/meilleur" sans JSON-LD Offer => editorial, rejet
  - word_count > 1200 sans panier ni JSON-LD Offer => page contenu, rejet
  - Signaux insuffisants => rejet
- **Integration discovery** : is_bad_url applique AVANT scraping, is_product_page applique APRES scraping
- **Debug admin** : section "Validateur de page produit" dans admin pour tester une URL et voir tous les signaux
- Non-whey products are filtered out by checking whey keywords in title/URL/H1
- Price validation: range 8-200 EUR, old/crossed-out prices excluded, price_per_kg >200 EUR/kg flagged as suspicious

Sites exclus : Amazon (503), Decathlon (403), reseaux sociaux, sites media/sante (doctissimo, passeportsante, etc.)

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
- Score prix (15% de la note finale) calcule via calculate_price_score_10() sur 12 paliers. score_global legacy (60/40) conserve pour compatibilite.
- Legacy scan system (scans + scan_items) coexiste avec le nouveau systeme Product/Offer pour compatibilite.
