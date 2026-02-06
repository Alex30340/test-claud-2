import os
import sys
import pandas as pd
from datetime import datetime

from scraper import scrape_products
from scoring import calculate_price_score, calculate_nutrition_score


def main():
    api_key = os.environ.get("BRAVE_API_KEY", "") or os.environ.get("BRAVE_SEARCH_API_KEY", "")

    if not api_key:
        print("Erreur : la variable d'environnement BRAVE_SEARCH_API_KEY n'est pas définie.")
        print("Obtenez une clé gratuite sur https://brave.com/search/api/")
        sys.exit(1)

    def progress(current, total, detail=""):
        print(f"  [{current + 1}/{total}] {detail}")

    def status(msg):
        print(f"\n>> {msg}")

    print("=" * 60)
    print("  Comparateur de Protéines en Poudre - Scan du marché")
    print("=" * 60)

    products = scrape_products(
        api_key=api_key,
        progress_callback=progress,
        status_callback=status,
    )

    if not products:
        print("\nAucun produit trouvé.")
        sys.exit(0)

    df = pd.DataFrame(products)

    csv_path = "market_snapshot.csv"
    df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

    print(f"\n{'=' * 60}")
    print(f"  {len(products)} produits trouvés")
    print(f"  Fichier CSV exporté : {csv_path}")
    print(f"{'=' * 60}")

    print(f"\n{'Produit':<50} {'Prix/kg':>10} {'Prot/100g':>10} {'Score':>8}")
    print("-" * 80)
    for p in products[:20]:
        name = (p["nom"] or "")[:48]
        ppk = f"{p['prix_par_kg']:.2f}" if p.get("prix_par_kg") else "N/A"
        prot = f"{p['proteines_100g']:.1f}g" if p.get("proteines_100g") else "N/A"
        score = f"{p['score_global']:.1f}" if p.get("score_global") else "N/A"
        print(f"  {name:<48} {ppk:>10} {prot:>10} {score:>8}")

    print(f"\nTerminé à {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
