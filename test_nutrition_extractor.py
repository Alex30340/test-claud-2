import unittest
from nutrition_extractor import (
    extract_nutrition_table,
    extract_serving_info,
    extract_protein_per_100g,
    validate_protein_value,
    extract_protein_from_jsonld,
)


class TestNutritionExtractor(unittest.TestCase):

    def test_table_per_100g_gold_standard(self):
        html = """
        <html><body>
        <h2>Gold Standard 100% Whey</h2>
        <table>
            <tr><th>Valeurs nutritionnelles</th><th>Pour 100 g</th><th>Par portion (31g)</th></tr>
            <tr><td>Calories</td><td>375 kcal</td><td>116 kcal</td></tr>
            <tr><td>Protéines</td><td>77.0 g</td><td>24 g</td></tr>
            <tr><td>Glucides</td><td>8.0 g</td><td>2.5 g</td></tr>
            <tr><td>Lipides</td><td>5.0 g</td><td>1.6 g</td></tr>
        </table>
        </body></html>
        """
        result = extract_protein_per_100g(html)
        self.assertIsNotNone(result["protein_per_100g"])
        self.assertAlmostEqual(result["protein_per_100g"], 77.0, places=1)
        self.assertEqual(result["protein_source"], "table")
        self.assertGreaterEqual(result["protein_confidence"], 0.9)
        self.assertFalse(result["protein_suspect"])

    def test_serving_conversion(self):
        html = """
        <html><body>
        <h2>Whey Protein Isolate</h2>
        <div class="nutrition-facts">
            <p>Taille de la portion : 31 g (1 scoop)</p>
            <table>
                <tr><th>Nutriments</th><th>Par portion</th></tr>
                <tr><td>Protéines</td><td>24 g</td></tr>
                <tr><td>Glucides</td><td>3 g</td></tr>
            </table>
        </div>
        </body></html>
        """
        result = extract_protein_per_100g(html)
        self.assertIsNotNone(result["protein_per_100g"])
        expected = round((24 / 31) * 100, 1)
        self.assertAlmostEqual(result["protein_per_100g"], expected, places=0)
        self.assertFalse(result["protein_suspect"])

    def test_marketing_100_whey_rejected(self):
        html = """
        <html><body>
        <h1>100% Whey Protein - Meilleure qualite</h1>
        <p>Notre 100% Whey est composee de proteines de haute qualite.</p>
        <p>100% pure whey protein pour des resultats optimaux.</p>
        </body></html>
        """
        result = extract_protein_per_100g(html)
        self.assertNotEqual(result["protein_per_100g"], 100)

    def test_validate_protein_value_96_suspect(self):
        val, suspect = validate_protein_value(96.0)
        self.assertIsNone(val)
        self.assertTrue(suspect)

    def test_validate_protein_value_100_suspect(self):
        val, suspect = validate_protein_value(100.0)
        self.assertIsNone(val)
        self.assertTrue(suspect)

    def test_validate_protein_value_30_too_low(self):
        val, suspect = validate_protein_value(30.0)
        self.assertIsNone(val)
        self.assertFalse(suspect)

    def test_validate_protein_value_77_valid(self):
        val, suspect = validate_protein_value(77.0)
        self.assertEqual(val, 77.0)
        self.assertFalse(suspect)

    def test_jsonld_extraction(self):
        jsonld = {
            "nutrition": {
                "proteinContent": "80 g"
            }
        }
        result = extract_protein_from_jsonld(jsonld)
        self.assertEqual(result["protein_per_100g"], 80.0)
        self.assertEqual(result["protein_source"], "jsonld")
        self.assertFalse(result["protein_suspect"])

    def test_jsonld_suspect_value(self):
        jsonld = {
            "nutrition": {
                "proteinContent": "100 g"
            }
        }
        result = extract_protein_from_jsonld(jsonld)
        self.assertIsNone(result["protein_per_100g"])
        self.assertTrue(result["protein_suspect"])

    def test_extract_nutrition_table_direct(self):
        html = """
        <html><body>
        <table>
            <tr><th>Nutriment</th><th>Pour 100g</th></tr>
            <tr><td>Protéines</td><td>82,5 g</td></tr>
        </table>
        </body></html>
        """
        result = extract_nutrition_table(html)
        self.assertIsNotNone(result["protein_per_100g"])
        self.assertAlmostEqual(result["protein_per_100g"], 82.5, places=1)

    def test_extract_serving_info(self):
        html = """
        <html><body>
        <p>Dose : 30 g</p>
        <p>Protéines par dose : 25 g</p>
        </body></html>
        """
        result = extract_serving_info(html)
        self.assertEqual(result["serving_size_g"], 30.0)
        self.assertEqual(result["protein_per_serving_g"], 25.0)
        expected = round((25 / 30) * 100, 1)
        self.assertAlmostEqual(result["calculated_protein_per_100g"], expected, places=1)


if __name__ == "__main__":
    unittest.main()
