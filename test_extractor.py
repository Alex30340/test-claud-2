import pytest
from bs4 import BeautifulSoup
from extractor import extract_price, extract_currency, extract_weight_kg, detect_needs_js_render
from validator import validate_price, validate_weight, compute_confidence_v2


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_extract_price_jsonld():
    html = '''
    <html><head>
    <script type="application/ld+json">
    {"@type": "Product", "name": "Whey Isolate 1kg",
     "offers": {"@type": "Offer", "price": "39.90", "priceCurrency": "EUR"}}
    </script>
    </head><body><p>Test</p></body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 39.90
    assert "jsonld" in source


def test_extract_price_jsonld_price_specification():
    html = '''
    <html><head>
    <script type="application/ld+json">
    {"@type": "Product", "name": "Whey Native",
     "offers": {"@type": "Offer",
       "priceSpecification": {"@type": "PriceSpecification", "price": 45.50}}}
    </script>
    </head><body><p>Test</p></body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 45.50
    assert "jsonld" in source


def test_extract_price_og_meta():
    html = '''
    <html><head>
    <meta property="product:price:amount" content="29.99" />
    <meta property="product:price:currency" content="EUR" />
    </head><body><p>Whey protein</p></body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 29.99
    assert "meta" in source


def test_extract_price_next_data():
    html = '''
    <html><head></head><body>
    <script id="__NEXT_DATA__" type="application/json">
    {"props": {"pageProps": {"product": {"name": "Whey", "price": 34.90}}}}
    </script>
    </body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 34.90
    assert source == "next_data"


def test_extract_price_regex_fallback():
    html = '''
    <html><head></head><body>
    <div class="product-info">
        <h1>Whey Isolate Premium</h1>
        <span class="current-price">49,90 €</span>
        <del class="old-price">59,90 €</del>
        <button>Ajouter au panier</button>
    </div>
    </body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 49.90
    assert source == "html_priority"


def test_no_price_needs_js_render():
    html = '''
    <html><head></head><body>
    <div id="__next"></div>
    <script id="__NEXT_DATA__" type="application/json">
    {"props": {"pageProps": {"product": {"name": "Whey Native"}}}}
    </script>
    <script src="/_next/static/chunks/main.js"></script>
    <button>Ajouter au panier</button>
    </body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price is None

    needs_js = detect_needs_js_render(soup, has_price=False)
    assert needs_js is True


def test_extract_currency_from_jsonld():
    html = '''
    <html><head>
    <script type="application/ld+json">
    {"@type": "Product", "name": "Whey",
     "offers": {"@type": "Offer", "price": 30, "priceCurrency": "GBP"}}
    </script>
    </head><body></body></html>
    '''
    soup = _soup(html)
    currency = extract_currency(soup)
    assert currency == "GBP"


def test_extract_currency_default_eur():
    html = '<html><body><p>Prix : 30 €</p></body></html>'
    soup = _soup(html)
    currency = extract_currency(soup)
    assert currency == "EUR"


def test_extract_weight_kg_from_title():
    html = '<html><body><h1>Whey Isolate 2kg Chocolat</h1></body></html>'
    soup = _soup(html)
    weight = extract_weight_kg(soup, "Whey Isolate 2kg Chocolat")
    assert weight == 2.0


def test_extract_weight_kg_grams():
    html = '<html><body><h1>Whey Isolate 750g Vanille</h1></body></html>'
    soup = _soup(html)
    weight = extract_weight_kg(soup, "Whey Isolate 750g Vanille")
    assert weight == 0.75


def test_extract_weight_out_of_range():
    html = '<html><body><h1>Whey Isolate 10kg</h1></body></html>'
    soup = _soup(html)
    weight = extract_weight_kg(soup, "Whey Isolate 10kg")
    assert weight is None


def test_validate_price():
    assert validate_price(29.99) == 29.99
    assert validate_price(5.0) is None
    assert validate_price(300.0) is None
    assert validate_price(None) is None


def test_validate_weight():
    assert validate_weight(1.0) == 1.0
    assert validate_weight(0.1) is None
    assert validate_weight(6.0) is None
    assert validate_weight(None) is None


def test_compute_confidence_v2_with_js_render():
    data = {"nom": "Whey Test Product Name", "prix": None, "poids_kg": 1.0, "proteines_100g": 80}
    conf = compute_confidence_v2(data, has_jsonld=True, needs_js_render=True)
    assert conf <= 0.3

    data_with_price = {"nom": "Whey Test Product", "prix": 30.0, "poids_kg": 1.0, "proteines_100g": 80, "prix_par_kg": 30.0}
    conf2 = compute_confidence_v2(data_with_price, has_jsonld=True, needs_js_render=False)
    assert conf2 > 0.7


def test_ignore_crossed_price():
    html = '''
    <html><head></head><body>
    <div class="product">
        <del><span class="price">59,90 €</span></del>
        <span class="current-price">39,90 €</span>
    </div>
    </body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 39.90


def test_extract_price_jsonld_type_list():
    html = '''
    <html><head>
    <script type="application/ld+json">
    {"@type": ["Product", "Thing"], "name": "Whey Premium",
     "offers": {"@type": "Offer", "price": "52.00", "priceCurrency": "EUR"}}
    </script>
    </head><body><p>Test</p></body></html>
    '''
    soup = _soup(html)
    price, source = extract_price(soup)
    assert price == 52.00
    assert "jsonld" in source


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
