# Comparateur de Protéines en Poudre

## Overview
Application Python/Streamlit qui analyse automatiquement le marché des protéines en poudre (whey) en France. Utilise l'API Brave Search pour trouver des produits, extrait les données via JSON-LD schema.org, calcule des scores prix/nutrition, et exporte en CSV/Excel.

## Architecture

### Files
- `app.py` - Interface Streamlit (dashboard, tableau de classement, export CSV/Excel)
- `main.py` - Script CLI pour exécution en ligne de commande
- `scraper.py` - Module de recherche (Brave Search API) et extraction de données produits
- `scoring.py` - Calcul des scores prix/valeur, nutrition, et global
- `.streamlit/config.toml` - Configuration Streamlit (port 5000)

### Dependencies
- streamlit, httpx, beautifulsoup4, pandas, lxml, openpyxl

### Required Secrets
- `BRAVE_SEARCH_API_KEY` - Clé API Brave Search (https://brave.com/search/api/)

### Scoring System
- Score Prix: 20 EUR/kg = 100, 80 EUR/kg = 0 (interpolation linéaire)
- Score Nutrition: 90g prot/100g = 100, 60g prot/100g = 0
- Score Global: 50% prix + 50% nutrition

### Running
- Streamlit UI: `streamlit run app.py --server.port 5000`
- CLI: `python main.py` (requires BRAVE_SEARCH_API_KEY env var)
