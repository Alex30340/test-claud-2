# ProteinScan - Comparateur de Proteines Whey SaaS

## Overview
ProteinScan is a SaaS application built with Python and Streamlit, designed to compare whey protein products in France. It provides objective product scoring based on nutritional value, health considerations, and price to help users make informed purchasing decisions. The platform features a robust product engine with automated discovery and refresh pipelines to maintain an up-to-date catalog, a comprehensive scoring system, and a searchable product catalog. The business vision is to become the leading platform for protein product comparison, with ambitions to expand into personalized nutrition and diet recommendations.

## User Preferences
The user prefers clear, concise communication. They value iterative development and expect to be consulted before major architectural or feature changes. They prioritize a well-structured, maintainable codebase and detailed explanations for complex logic.

## System Architecture

### Core Design Principles
The system separates stable product attributes from volatile offer details, allowing one product to have multiple offers. Automated data pipelines (Discovery and Refresh) ensure the catalog is current, with a confidence scoring system for data quality. Data extraction uses a multi-source approach, prioritizing structured data, falling back to dynamic content parsing, regex, and OCR (GPT-4o vision) for nutrition. Strict validation ensures scraped URLs are actual product pages. The catalog displays only products with complete data.

### UI/UX Decisions
The application features a simplified navigation with public and authenticated modes. The default "Catalogue" provides a searchable product list with advanced filters and export options. A "Comparateur" allows side-by-side comparison of up to 5 products with pre-filled suggestions and shareable links. An "Administration" section provides pipeline controls and a data quality dashboard. Public pages include a landing page, login, and registration. Product detail pages feature nutrition tables, aminograms, image galleries, price history charts, price alerts, reviews, community recommendations, and personalized scores. The UI includes visual scoring (1-10 scale), detailed product cards, and a light/dark theme toggle.

### Technical Implementations & Features
-   **Scoring System (V3)**: A weighted scoring model (Protein 50%, Health 35%, Price 15%) calculates a final product score, with potential premium bonuses.
-   **Personalized Score**: Users can adjust the weight distribution of Protein, Health, and Price scores to generate a personalized product score.
-   **Advanced Catalog Filters**: Filters include minimum protein, maximum price/kg, and minimum score, in addition to text search and whey type.
-   **Price History & Alerts**: Tracks price changes and notifies users when a product's price drops below a set target.
-   **Product Image Gallery**: Stores and displays multiple images per product.
-   **Social Sharing**: Enables sharing product and comparison links via query parameters.
-   **Product Detection**: Automated identification of whey type, manufacturing origin, and specific additives.
-   **Multi-Source Nutrition & Aminogram Extraction**: Utilizes a fusion engine to extract and cross-check nutrition data from structured data, HTML tables, and OCR, including full 18 amino acid profiles.
-   **Community Recommendations**: Allows users to provide recommendations with usage context, level, pros, and cons.
-   **Database Performance**: Optimized with connection pooling and extensive use of database indexes. Streamlit caching is used for frequently accessed data with appropriate TTLs.

### System Design Choices
-   **PostgreSQL Database**: Serves as the primary data store for all application data.
-   **Playwright Integration**: Used for headless browser automation to scrape JavaScript-heavy websites, handling cookie banners, accordion expansions, and lazy-loaded content.
-   **Smart Upsert Logic**: Prevents overwriting previously extracted data with empty values during product updates.
-   **Robust Image & Ingredient Extraction**: Employs fallbacks and multiple strategies to ensure comprehensive data capture.

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
-   **PostgreSQL**: Primary database.
-   **Brave Search API**: Used for product discovery.
-   `openai`: For GPT-4o vision OCR (via Replit AI Integrations).