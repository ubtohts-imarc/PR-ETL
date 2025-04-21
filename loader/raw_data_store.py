from sqlalchemy.orm import Session
from models.input import ProductInput
from models.raw_data import PriceRaw
from models.metadata import Product
from loader.base_loader import BaseLoader
from datetime import datetime
import pandas as pd
from utility.logger import get_logger

logger = get_logger()

class RawPriceWriter(BaseLoader):
    def __init__(self, df: pd.DataFrame, source_name: str):
        super().__init__()
        self.df = df
        self.source_name = source_name.strip().lower()

    def get_product_input_map(self) -> dict:
        """
        Maps product name -> product_input.id for the given source_name.
        """
        inputs = (
            self.session.query(ProductInput)
            .join(Product, Product.id == ProductInput.product_id)
            .filter(ProductInput.source_name == self.source_name)
            .all()
        )

        mapping = {}
        for input_obj in inputs:
            product_name = input_obj.product.name.strip().lower()
            mapping[product_name] = input_obj.id

        logger.info(f"ProductInput mapping: {mapping}")
        return mapping

    def save(self):
        try:
            mapping = self.get_product_input_map()
            logger.info(f"Mapping: {mapping}")

            records = []
            for _, row in self.df.iterrows():
                product_name = str(row.get("product", "")).strip().lower()
                price_value = row.get("price_value") or row.get("Price")
                price_date = row.get("price_date") or row.get("Date")
                category = row.get("product_category")

                config_id = mapping.get(product_name)
                if not config_id:
                    # logger.warning(f"Skipping row: product '{product_name}' not found in input config.")
                    continue
                
                logger.info(f"Processing product: {product_name}, config_id: {config_id}")
                record = PriceRaw(
                    source_name=self.source_name,
                    price_date=pd.to_datetime(price_date),
                    price_value=float(str(price_value).replace(",", "").replace(" ", "")),
                    product_category=category,
                    product_config_id=config_id,
                    last_update=datetime.utcnow()
                )
                records.append(record)

            logger.info(f"Total records to insert: {len(records)}, Sample: {records[:5]}")
            if records:
                self.session.add_all(records)
                self.session.commit()
                logger.info(f"Inserted {len(records)} records into raw_data.products_raw_data")
            else:
                logger.warning("No records to insert.")

        except Exception as e:
            logger.error(f"Error saving raw prices: {str(e)}")
            self.session.rollback()
        finally:
            self.session.close()
