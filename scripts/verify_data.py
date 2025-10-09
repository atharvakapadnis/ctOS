"""
Verify the ingested data
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.ingestion import ProductDatabase, export_debug_sample


def main():
    db = ProductDatabase()

    # Get a sample product
    print("Getting sample products...")
    unprocessed = db.get_unprocessed_products(limit=5)

    print(f"\nFirst 5 unprocessed products:")
    for product in unprocessed:
        print(f"  - {product.item_id}: {product.item_description[:50]}...")

    # Export sample to JSON
    print("\nExporting 10 sample records to JSON...")
    export_debug_sample(n=10)
    print("Sample exported to: data/debug/sample.json")

    # Get product by ID
    if unprocessed:
        first_id = unprocessed[0].item_id
        print(f"\nGetting product by ID: {first_id}")
        product = db.get_product_by_id(first_id)
        print(f"  Item: {product.item_id}")
        print(f"  Description: {product.item_description}")
        print(f"  HTS: {product.final_hts}")
        print(f"  Processed: {product.enhanced_description is not None}")


if __name__ == "__main__":
    main()
