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
- **UI/UX**: The Streamlit interface provides a user-friendly experience with:
    -   Clear navigation (Landing, Login, Register, Dashboard, Catalog, Search, Comparator, Product Detail, Admin).
    -   Visual scoring (1-10 scale, star ratings), colored confidence badges, and clear explanations for scores.
    -   Detailed product cards displaying protein, health, and price scores, BCAA/leucine metrics, and key attributes (type, origin, additives).
    -   Advanced filtering and sorting options for the product catalog.

### Key Features
- **Scoring System (V3)**:
    -   **Protein Score (50%)**: Based on protein percentage, BCAA/100g protein, and Leucine/100g protein, with quality control for BCAA ratios.
    -   **Health Score (35%)**: Starts at 10, with deductions for artificial sweeteners, flavors, thickeners, colorants, and high ingredient count.
    -   **Price Score (15%)**: Stepwise scoring based on price per kilogram.
    -   **Final Score**: Weighted average of the three main scores, with optional premium bonuses (e.g., high protein content, full aminogram, French origin).
- **Product Detection**: Automated identification of whey type (Native, Isolate, Hydrolysate, Concentrate), French manufacturing, origin, and presence of specific additives (sweeteners, flavors, thickeners, colorants).
- **Multi-Source Nutrition Extraction** (integrated in scraper.py):
    - Source A: Structured data (JSON-LD nutrition, additionalProperty) → confidence 0.85-0.9
    - Source B: HTML tables and div sections → confidence 0.65-0.9
    - Source C: OCR via GPT-4o vision on nutrition label images → confidence 0.55-0.65, triggered only when protein/kcal/aminogram missing
    - Fusion engine picks highest-confidence source per field, cross-checks macro coherence (kcal vs P+C+F)
    - `_match_field` uses longest-match-wins to prevent alias conflicts (e.g., "isoleucine" vs "leucine")
    - `_extract_from_table` deduplicates fields (seen_fields set), prefers "pour 100g de protéine" column for amino, scans context (siblings, parent headings) for amino_base detection
    - `_extract_from_div_sections` has sanity bounds (amino 0.01-50g, nutrition 0-100g, kcal 50-800) and max text length filters to prevent false positives
    - `_compute_bcaa_per_100g_prot` in scraper.py: smart inference for unknown amino_base using value range heuristics
    - Re-analysis pipeline recalculates protein/health/global/final scores after updating nutrition data
    - Extended DB columns: carbs_per_100g, sugar_per_100g, fat_per_100g, sat_fat_per_100g, kcal_per_100g, salt_per_100g, fiber_per_100g, amino_profile (JSONB), amino_base, raw_evidence (JSONB), nutrition_sources, macro_coherent, glutamine_g, arginine_g, lysine_g
- **Aminogram Extraction**: Full 18 amino acid profile (Leucine, Isoleucine, Valine, Glutamine, Arginine, Lysine, Methionine, Phenylalanine, Threonine, Tryptophan, Histidine, Alanine, Glycine, Proline, Serine, Tyrosine, Aspartic acid, Cysteine) with base detection (per_100g_protein/per_100g/per_serving).
- **JavaScript Render Detection**: Identifies pages requiring JavaScript rendering and adjusts confidence scores accordingly.
- **Performance**: Utilizes `ThreadPoolExecutor` for parallel extraction and includes delays/timeouts for external API calls and HTTP requests.
- **Database Performance**:
    - Connection pooling via `psycopg2.pool.ThreadedConnectionPool` (2-10 connections). Use `get_connection()` / `release_connection(conn)` pattern.
    - 7 database indexes on frequently queried columns (offers.product_id, reviews.product_id, products.score_final, etc.)
    - Streamlit `@st.cache_data` wrappers (TTL 30-60s) for catalog, product, offers, reviews queries — prefixed `cached_*`
    - Catalog query selects only needed columns (excludes amino_profile, raw_evidence, ingredients JSONB)

## External Dependencies
-   `streamlit`: Main framework for the web application UI.
-   `httpx`: Asynchronous HTTP client for making web requests.
-   `beautifulsoup4`: Library for parsing HTML and XML documents.
-   `pandas`: Data manipulation and analysis.
-   `lxml`: High-performance HTML/XML parser.
-   `openpyxl`: Library for reading/writing Excel files (used for data export).
-   `bcrypt`: Password hashing and verification.
-   `psycopg2-binary`: PostgreSQL database adapter.
-   **PostgreSQL**: Primary database for storing user accounts, product data, offers, and pipeline run history.
-   **Brave Search API**: Used for the Discovery pipeline to find new product URLs. (Requires `BRAVE_SEARCH_API_KEY` secret).
-   `openai`: OpenAI client for GPT-4o vision OCR (via Replit AI Integrations, no API key needed).
-   **GPT-4o Vision (via `multi_source_extractor.py`)**: OCR-based nutrition/aminogram extraction from product label images. Triggered when protein/kcal/amino data is missing from HTML sources.

## Admin Features
- **Discovery Pipeline**: Brave Search → scrape → validate → score → upsert products/offers
- **Refresh Pipeline**: Re-scrape active offers for price/availability updates
- **Re-analysis Pipeline**: Re-scrape existing products to populate extended nutrition fields (aminogram, macros) using multi_source_extractor. Targets products where amino_profile IS NULL or kcal_per_100g IS NULL.