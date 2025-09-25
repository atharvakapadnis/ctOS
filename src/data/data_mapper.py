"""
Map CSV data to database models for ctOS Service 1
"""

import pandas as pd
from typing import Dict, List
from datetime import datetime

from core.models import Product
from utils.logger import setup_logger
from utils.exceptions import DataValidationError

logger = setup_logger("ctOS.mapper")

# CSV column to model field mapping
CSV_TO_MODEL_MAPPING = {
    "item_id": "item_id",
    "item_description": "item_description",
    "product_group": "product_group",
    "product_group_description": "product_group_description",
    "product_group_code": "product_group_code",
    "material_class": "material_class",
    "material_detail": "material_detail",
    "manf_class": "manf_class",
    "supplier_id": "supplier_id",
    "supplier_name": "supplier_name",
    "country_of_origin": "country_of_origin",
    "import_type": "import_type",
    "port_of_delivery": "port_of_delivery",
    "final_hts": "final_hts",
    "hts_description": "hts_description",
}

REQUIRED_FIELDS = ["item_id", "item_description"]


class DataMapper:
    """CSV to model mapping functionality"""

    def __init__(self):
        pass

    def csv_row_to_product(self, row: pd.Series) -> Product:
        """Convert CSV row to product model"""
        try:
            """Validate required fields"""
            for field in REQUIRED_FIELDS:
                if (
                    field not in row
                    or pd.isna(row[field])
                    or str(row[field]).strip() == ""
                ):
                    raise DataValidationError(
                        f"Required firld {field} is missing or empty"
                    )

                # Map CSV column to product model fields
                product_data = {}
                for csv_col, model_field in CSV_TO_MODEL_MAPPING.items():
                    if csv_col in row:
                        value = row[csv_col]
                        # Handle nan values
                        if pd.isna(value):
                            product_data[model_field] = None
                        else:
                            # Clean string values
                            product_data[model_field] = (
                                str(value).strip if value else None
                            )

                return Product(**product_data)

        except Exception as e:
            logger.error(f"Error mapping CSV row to product: {e}")
            raise DataValidationError(f"Row mapping failed: {e}")

    def product_to_db_dict(self, product: Product) -> Dict:
        """Convert product to database dictionary"""
        return {
            "item_id": product.item_id,
            "item_description": product.item_description,
            "product_group": product.product_group,
            "product_group_description": product.product_group_description,
            "product_group_code": product.product_group_code,
            "material_class": product.material_class,
            "material_detail": product.material_detail,
            "manf_class": product.manf_class,
            "supplier_id": product.supplier_id,
            "supplier_name": product.supplier_name,
            "country_of_origin": product.country_of_origin,
            "import_type": product.import_type,
            "port_of_delivery": product.port_of_delivery,
            "final_hts": product.final_hts,
            "hts_description": product.hts_description,
            "created_at": product.created_at,
            "updated_at": product.updated_at,
        }

    def batch_map_products(self, df: pd.DataFrame) -> List[Product]:
        """Batch convert Dataframe to products"""
        products = []
        errors = []

        for idx, row in df.iterrows():
            try:
                product = self.csv_row_to_product(row)
                products.append(product)
            except Exception as e:
                errors.append(f"Row {idx}: {e}")

        if errors:
            logger.warning(
                f"Mapping errors encountered: {len(errors)} out of {len(df)} rows"
            )
            for error in errors[:5]:
                logger.warning(error)

        logger.info(
            f"Successfully mapped {len(products)} products out of {len(df)} rows"
        )
        return products
