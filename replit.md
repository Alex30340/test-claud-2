# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
ProteinScan is a SaaS application built with Python and Streamlit, designed to compare whey protein products available in France. Its primary purpose is to help users make informed decisions by providing objective product scoring based on nutritional value, health considerations, and price.

The platform features a robust product engine with a clear separation between stable product data and volatile offer data. It includes automated discovery and refresh pipelines to maintain an up-to-date catalog. Key capabilities include a comprehensive scoring system (protein, health, price), a searchable product catalog with confidence scores, and an administrative interface for managing the system.

The business vision is to become the leading platform for protein product comparison, with future ambitions to expand into personalized nutrition, muscle programs, and diet recommendations based on user profiles.

## User Preferences
The user prefers clear, concise communication. They value iterative development and expect to be consulted before major architectural or feature changes. They prioritize a well-structured, maintainable codebase and detailed explanations for complex logic. The user also specified to not integrate Stripe for now.

## System Architecture

### Core Design Principles
- **Product/Offer Separation**: Stable product attributes (nutrition, ingredients, scores) are distinct from volatile offer details (price, merchant, availability). A product can have multiple offers from different merchants.
- **Automated Data Pipelines**:
    - **Discovery Pipeline**: Weekly process using Brave Search API to identify new whey protein products, scrape their data, and add them to the catalog with an initial confidence score. It includes configurable parameters for domain limiting and brand seeding.
    - **Refresh Pipeline**: Daily process to re-scrape active offers, updating prices and availability. Offers failing three consecutive refreshes are marked inactive.
- **Confidence Scoring**: A composite score (0-1) is assigned to each offer based on the presence and quality of extracted data (JSON-LD, price, weight, protein, price/kg, product name length).
- **Multi-Source Data Extraction**: A layered approach for extracting product data:
    1.  **Structured Data**: JSON-LD (schema.org Product), Open Graph meta tags.
    2.  **Dynamic Content**: JSON scripts from Next.js/Nuxt applications.
    3.  **Regex Fallback**: HTML pattern matching for prices, ingredients, and nutritional information.
    4.  **Advanced Nutrition Extraction**: Utilizes a pipeline combining structured data, HTML tables, and OCR (GPT-4o vision) for comprehensive macro and aminogram analysis.
- **Strict Product Page Validation**: A `page_validator.py` module ensures that scraped URLs correspond to actual product pages with purchase proof, filtering out blogs, articles, and irrelevant content.
- **Curated Catalog**: Only products with complete data (protein, BCAA, score) are displayed. Quality threshold: `score_final IS NOT NULL`. Catalog cleaned from ~108 to ~66 scored products after deduplication.
- **UI/UX**: Simplified 3-page navigation for authenticated users:
    -   **Catalogue** (default): Searchable product list with filters (type whey, sort, toggles), pagination (20/page), product cards with scores. Export buttons (CSV/Excel) visible directly below the table.
    -   **Comparateur**: Side-by-side comparison of up to 5 products. Pre-filled suggestions when empty (Top 3 Isolate/Native, Meilleur rapport qualite/prix, Top 3 sans edulcorant). Export comparison to CSV + share IDs.
    -   **Administration**: Pipeline controls, data quality dashboard, catalog management.
    -   Public pages: Landing (with Top 5 products preview), Login, Register.
    -   Sub-pages: Product Detail (from catalogue) with nutrition table, aminogram, reviews, and community recommendations.
    -   Visual scoring (1-10 scale, star ratings), detailed product cards with protein, health, price scores, BCAA/leucine metrics, badges.

### Key Features
- **Scoring System (V3)**:
    -   **Protein Score (50%)**: Based on protein percentage, BCAA/100g protein, and Leucine/100g protein, with quality control for BCAA ratios.
    -   **Health Score (35%)**: Starts at 10, with deductions for artificial sweeteners, flavors, thickeners, colorants, and high ingredient count.
    -   **Price Score (15%)**: Stepwise scoring based on price per kilogram.
    -   **Final Score**: Weighted average of the three main scores, with optional premium bonuses (e.g., high protein content, full aminogram, French origin).
- **Product Detection**: Automated identification of whey type (Native, Isolate, Hydrolysate, Concentrate), French manufacturing, origin, and presence of specific additives (sweeteners, flavors, thickeners, colorants). Whey type detection prioritizes product name (with whey/protein context check) over page text; page text fallback requires 2+ keyword hits or contextual proximity to whey-related terms to avoid false positives. Protein validation threshold: values <15g/100g rejected, >=96g rejected as suspect. Per-serving values (typically 15-35g) are filtered out.
- **Multi-Source Nutrition Extraction** (integrated in scraper.py):
    - Source A: Structured data (JSON-LD nutrition, additionalProperty) -> confidence 0.85-0.9
    - Source B: HTML tables and div sections -> confidence 0.65-0.9
    - Source C: OCR via GPT-4o vision on nutrition label images -> confidence 0.55-0.65, triggered only when protein/kcal/aminogram missing
    - Fusion engine picks highest-confidence source per field, cross-checks macro coherence (kcal vs P+C+F)
- **Aminogram Extraction**: Full 18 amino acid profile with base detection (per_100g_protein/per_100g/per_serving).
- **Nutrition Table**: Product detail page displays a styled HTML nutrition table (pour 100g) with energy, protein (highlighted), carbs, sugars, fat, sat fat, fiber, salt.
- **Community Recommendations**: Users can write recommendations with usage context (Musculation, Endurance, Perte de poids, Sante generale, Recuperation), level (Debutant, Intermediaire, Avance, Tous niveaux), pros/cons, and comment. Displayed grouped by usage context on the product detail page.
- **Landing Page Top 5**: Public landing page shows the top 5 products by score_final with score, name, brand, and protein content. No login required.
- **Comparator Suggestions**: When comparator is empty, 3 pre-made comparisons are offered as clickable cards.
- **Export UX**: CSV/Excel export buttons are directly visible below the catalog table (no expander). Comparator has a dedicated CSV export button.
- **Database Performance**:
    - Connection pooling via `psycopg2.pool.ThreadedConnectionPool` (2-10 connections). Use `get_connection()` / `release_connection(conn)` pattern.
    - 8 database indexes (offers.product_id, reviews.product_id, products.score_final, recommendations.product_id, etc.)
    - Streamlit `@st.cache_data` wrappers: catalog TTL 300s, product/offers TTL 120s, reviews TTL 30s, stats TTL 300s
    - Catalog query selects only needed columns (excludes amino_profile, raw_evidence, ingredients JSONB)

## Database Tables
- **users**: id, email, display_name, password_hash, role, plan, created_at
- **products**: id, name, brand, proteines_100g, type_whey, score_final, score_proteique, score_sante, score_global, image_url, ingredients, ingredient_count, amino_profile (JSONB), kcal_per_100g, carbs_per_100g, sugar_per_100g, fat_per_100g, sat_fat_per_100g, salt_per_100g, fiber_per_100g, bcaa_per_100g_prot, leucine_g, isoleucine_g, valine_g, origin_label, made_in_france, has_sucralose, has_acesulfame_k, has_aspartame, has_artificial_flavors, has_thickeners, has_colorants, has_aminogram, mentions_bcaa, amino_base, raw_evidence (JSONB), nutrition_sources, macro_coherent, profil_suspect, protein_suspect
- **offers**: id, product_id (FK), url, merchant, prix, poids_kg, prix_par_kg, confidence, is_active, needs_js_render, fail_count, created_at, updated_at
- **reviews**: id, product_id (FK), user_id (FK), rating, title, comment, purchased_from, is_flagged, is_hidden, created_at
- **recommendations**: id, product_id (FK), user_id (FK), usage_context, level, pros, cons, comment, is_hidden, created_at
- **scans**: id, user_id (FK), query, status, details, created_at
- **scan_items**: id, scan_id (FK), product_id (FK), created_at
- **pipeline_runs**: id, pipeline_type, status, products_found, products_added, details, created_at

## External Dependencies
-   `streamlit`: Main framework for the web application UI.
-   `httpx`: Asynchronous HTTP client for making web requests.
-   `beautifulsoup4`: Library for parsing HTML and XML documents.
-   `pandas`: Data manipulation and analysis.
-   `lxml`: High-performance HTML/XML parser.
-   `openpyxl`: Library for reading/writing Excel files (used for data export).
-   `bcrypt`: Password hashing and verification.
-   `psycopg2-binary`: PostgreSQL database adapter.
-   `playwright`: Headless browser automation for JS-heavy sites. Requires Chromium + system deps (nspr, nss, mesa, etc.).
-   **PostgreSQL**: Primary database for storing user accounts, product data, offers, and pipeline run history.
-   **Brave Search API**: Used for the Discovery pipeline to find new product URLs. (Requires `BRAVE_SEARCH_API_KEY` secret).
-   `openai`: OpenAI client for GPT-4o vision OCR (via Replit AI Integrations, no API key needed).

## Admin Features
- **Discovery Pipeline**: Brave Search -> scrape -> validate -> score -> upsert products/offers
- **Refresh Pipeline**: Re-scrape active offers for price/availability updates
- **Re-analysis Pipeline**: Re-scrape existing products to populate extended nutrition fields (aminogram, macros) using multi_source_extractor. Targets products where amino_profile IS NULL or kcal_per_100g IS NULL.
- **Data Quality Dashboard**: Admin section showing catalog completeness (protein, ingredients, score, image, BCAA coverage) with breakdown by missing field and list of incomplete products.
- **Catalog Cleanup**: Removes non-product entries (category pages, brand homepages, entries with no protein + no ingredients + no score). Uses `_BAD_PRODUCT_NAME_PATTERNS` regex list.
- **Re-scrape Incomplets**: Pipeline that re-scrapes products with missing data (protein, ingredients, images) using improved extraction logic. Supports optional Playwright browser mode.

## Browser Scraper Architecture (`browser_scraper.py`)
- **Playwright Integration**: Headless Chromium for JS-heavy sites. Requires `LD_LIBRARY_PATH` pointing to mesa libgbm (`/nix/store/.../mesa-libgbm-.../lib`).
- **Cookie Dismissal**: Auto-clicks common GDPR/cookie banners (Accepter, Accept all, J'accepte, etc.)
- **Accordion/Tab Expansion**: Clicks elements matching nutrition keywords (composition, ingredients, valeurs nutritionnelles, aminogramme, voir plus, etc.) to reveal hidden content.
- **Lazy-load Scroll**: Scrolls page to trigger lazy-loaded images.
- **Carousel Image Extraction**: Collects images from product galleries, carousels, sliders; clicks thumbnails to reveal hidden images. Returns up to 30 filtered images.
- **Integration**: `extract_product_data(url, force_browser=True)` uses browser; auto-retries with browser when `needs_js` detected and critical data missing. Browser images are scored and top 5 added as OCR candidates.

## Data Quality Architecture
- **Smart Upsert (`upsert_product`)**: When updating existing products, fields in the `preserve_fields` set are never overwritten with None/empty values. This prevents re-scrapes from erasing previously extracted data.
- **Image Extraction Fallbacks**: `_extract_product_image_fallback()` checks: product:image meta -> image_src link -> CSS selectors (.product-image, .product-gallery, etc.) -> itemprop=image -> main content large images (>80px).
- **Ingredients Extraction**: `find_ingredients_block_html()` searches expanded heading patterns (7 patterns incl. "du produit", "nutritionnelle", "what's in it"), then class-based search, then itemprop. Text fallback uses 4 regex patterns.
