# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
ProteinScan is a SaaS application built with Python and Streamlit, designed to compare whey protein products in France. It provides objective product scoring based on nutritional value, health considerations, and price to help users make informed purchasing decisions. The platform features a robust product engine with automated discovery and refresh pipelines to maintain an up-to-date catalog, a comprehensive scoring system, and a searchable product catalog. The business vision is to become the leading platform for protein product comparison, with ambitions to expand into personalized nutrition and diet recommendations.

## User Preferences
The user prefers clear, concise communication. They value iterative development and expect to be consulted before major architectural or feature changes. They prioritize a well-structured, maintainable codebase and detailed explanations for complex logic.

## System Architecture

### Core Design Principles
The system separates stable product attributes from volatile offer details, allowing one product to have multiple offers. Automated data pipelines (Discovery and Refresh) ensure the catalog is current, with a confidence scoring system for data quality. Data extraction uses a multi-source approach, prioritizing structured data, falling back to dynamic content parsing, regex, and OCR (GPT-4o vision) for nutrition. Strict validation ensures scraped URLs are actual product pages. The catalog displays only products with complete data.

### UI/UX Decisions
The application features a simplified navigation with public and authenticated modes. The default "Catalogue" provides a searchable product list with advanced filters (sliders for score, protein, price), favorites filter, and export options. A max of 2 products per brand is shown in catalog by default (disabled during search or favorites view). A "Comparateur" allows side-by-side comparison of up to 5 products with pre-filled suggestions and shareable links. An "Administration" section provides pipeline controls, a data quality dashboard, and image gallery management. Public pages include a landing page (with educational "Pourquoi bien choisir sa whey" section), login, registration, and a "Mentions legales" page with health disclaimers and legal notices. Product detail pages feature nutrition tables, aminograms, image galleries, price history charts, price alerts, reviews, community recommendations, personalized scores, favorite toggle, and share links. The UI includes visual scoring (1-10 scale), detailed product cards, a light/dark theme toggle, and recently viewed products section. Product badges use `translate="no"` attribute to prevent browser auto-translation of technical terms (Native, Isolate, etc.).

### Technical Implementations & Features
-   **Scoring System (V3)**: A weighted scoring model (Protein 50%, Health 35%, Price 15%) calculates a final product score, with potential premium bonuses.
-   **Personalized Score**: Users can adjust the weight distribution of Protein, Health, and Price scores to generate a personalized product score. Stored in `user_preferences` table. Displayed on product detail page when weights differ from defaults.
-   **Advanced Catalog Filters**: Filters include minimum protein, maximum price/kg, and minimum score (sliders), in addition to text search, whey type, and favorites-only filter.
-   **Price History & Alerts**: Tracks price changes via `price_history` table (recorded during refresh pipeline) and notifies users when a product's price drops below a set target via `price_alerts` and `notifications` tables.
-   **Product Image Gallery**: `product_images` table stores multiple images per product. Gallery displayed on product detail page (up to 5 thumbnails). Browser scraper populates gallery during discovery. Admin can add/remove images manually.
-   **Social Sharing**: Enables sharing product (`?product=ID`) and comparison links (`?compare=1,2,3`) via query parameters. URLs parsed on load to auto-navigate.
-   **Favorites System**: `user_favorites` table allows users to favorite/unfavorite products. Heart/toggle button on catalog cards and product detail page. "Mes favoris uniquement" filter in catalog. Favorites count displayed in sidebar.
-   **Recently Viewed**: Session-state tracking of last 10 viewed products. "Vus recemment" section on catalog page with quick-access thumbnails.
-   **Search UX**: Result count displayed with search term. "No results" message with suggestions when empty. Clear search feedback.
-   **Product Detection**: Automated identification of whey type, manufacturing origin, and specific additives.
-   **Multi-Source Nutrition & Aminogram Extraction**: Utilizes a fusion engine to extract and cross-check nutrition data from structured data, HTML tables, and OCR, including full 18 amino acid profiles.
-   **Community Recommendations**: Allows users to provide recommendations with usage context, level, pros, and cons.
-   **Light/Dark Theme**: Toggle in sidebar. Light theme CSS overrides for white backgrounds, dark text, and blue accents.
-   **Database Performance**: Optimized with connection pooling and extensive use of database indexes. Streamlit caching is used for frequently accessed data with appropriate TTLs.
-   **Radar Chart Comparator**: Matplotlib-generated radar/spider chart comparing products on 5 axes (Proteines, Sante, Prix, BCAA, Leucine). Displayed in comparator when 2+ products selected.
-   **Thematic Rankings**: Auto-generated rankings on catalog page: "Meilleur rapport qualité/prix", "Top sans édulcorant", "Top Made in France", "Meilleur pour débutants". Toggleable via checkbox.
-   **Guide d'achat personnalisé**: Questionnaire (objectif, budget, sensibilités) recommending top 3 matching products. Accessible from sidebar nav and landing page.
-   **Anomaly Detection (Admin)**: Dashboard flagging products with suspicious data (protein > 95g, bad names, no price, duplicates, no image). Includes delete actions.
-   **Nouveautés Section**: Shows recently added products (last 30 days) at top of catalog page.
-   **User Badges System**: `user_badges` table. Badge types: first_review, top_reviewer, community_helper, price_hunter, collector. Awarded automatically on relevant actions. Displayed in sidebar.
-   **Enhanced Price History**: Price trend indicators (current vs historical), best/highest price metrics, improved chart on product detail page.
-   **Export TXT**: Text export of comparisons alongside existing CSV export.
-   **Auto-Rescrape (Admin)**: Re-scrape incomplete products (missing image, nutrition, score) with progress bar.
-   **Email Alert Preference**: Toggle in user preferences to opt-in for email notifications (placeholder for future email integration).

### System Design Choices
-   **PostgreSQL Database**: Serves as the primary data store for all application data.
-   **Playwright Integration**: Used for headless browser automation to scrape JavaScript-heavy websites, handling cookie banners, accordion expansions, and lazy-loaded content. Gallery images from browser scraper saved to `product_images` table.
-   **Smart Upsert Logic**: Prevents overwriting previously extracted data with empty values during product updates.
-   **Robust Image & Ingredient Extraction**: Employs fallbacks and multiple strategies to ensure comprehensive data capture.

## Database Tables
- **users**: id, email, display_name, password_hash, role, plan, created_at
- **products**: id, name, brand, proteines_100g, type_whey, score_final, score_proteique, score_sante, score_global, image_url, ingredients, ingredient_count, amino_profile (JSONB), kcal_per_100g, carbs_per_100g, sugar_per_100g, fat_per_100g, sat_fat_per_100g, salt_per_100g, fiber_per_100g, bcaa_per_100g_prot, leucine_g, isoleucine_g, valine_g, origin_label, made_in_france, has_sucralose, has_acesulfame_k, has_aspartame, has_artificial_flavors, has_thickeners, has_colorants, has_aminogram, mentions_bcaa, amino_base, raw_evidence (JSONB), nutrition_sources, macro_coherent, profil_suspect, protein_suspect
- **offers**: id, product_id (FK), url, merchant, prix, poids_kg, prix_par_kg, confidence, is_active, needs_js_render, fail_count, created_at, updated_at
- **reviews**: id, product_id (FK), user_id (FK), rating, title, comment, purchased_from, is_flagged, is_hidden, created_at
- **recommendations**: id, product_id (FK), user_id (FK), usage_context, level, pros, cons, comment, is_hidden, created_at
- **scans**: id, user_id (FK), query, status, details, created_at
- **scan_items**: id, scan_id (FK), product_id (FK), created_at
- **pipeline_runs**: id, pipeline_type, status, products_found, products_added, details, created_at
- **price_history**: id, product_id (FK), prix, prix_par_kg, merchant, recorded_at
- **price_alerts**: id, user_id (FK), product_id (FK), target_price, is_active, triggered_at, created_at
- **notifications**: id, user_id (FK), message, link_product_id, is_read, created_at
- **user_preferences**: id, user_id (FK, unique), weight_protein (default 50), weight_health (default 35), weight_price (default 15), updated_at
- **product_images**: id, product_id (FK), image_url, sort_order, created_at
- **user_favorites**: id, user_id (FK), product_id (FK), created_at, UNIQUE(user_id, product_id)
- **user_badges**: id, user_id (FK), badge_type (VARCHAR), earned_at, UNIQUE(user_id, badge_type)

## External Dependencies
-   `streamlit`: Web application UI framework.
-   `httpx`: Asynchronous HTTP client.
-   `beautifulsoup4`: HTML/XML parsing.
-   `pandas`: Data manipulation.
-   `lxml`: High-performance HTML/XML parser.
-   `openpyxl`: Excel file reading/writing.
-   `bcrypt`: Password hashing.
-   `psycopg2-binary`: PostgreSQL adapter.
-   `playwright`: Headless browser automation.
-   `matplotlib`: Radar chart generation for comparator.
-   **PostgreSQL**: Primary database.
-   **Brave Search API**: Used for product discovery.
-   `openai`: For GPT-4o vision OCR (via Replit AI Integrations).

## Admin Features
- Discovery, Refresh, Re-analysis, Re-scrape pipelines
- Data Quality Dashboard with completeness stats
- Catalog Cleanup (removes invalid entries)
- Moderation des avis (flagged review management)
- Discovery Health (domain/brand coverage)
- Image Gallery Management (add/remove images by product)
- Debug tools: Page Validator, URL Resolver
