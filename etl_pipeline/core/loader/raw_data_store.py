from datetime import datetime

import pandas as pd
from sqlalchemy.exc import IntegrityError

from db.models.input import ProductInput
from db.models.metadata import Product, Source
from db.models.raw_data import PriceRaw
from etl_pipeline.core.loader.base_loader import BaseLoader
from utility.logger import get_logger

logger = get_logger()

class RawPriceWriter(BaseLoader):
    def __init__(self, df: pd.DataFrame, source_name: str):
        super().__init__()
        self.df = df
        self.source_name = source_name.strip().lower()

    def get_product_input_map(self) -> dict:
        try:
            logger.info(f"Fetching product input mapping for source: {self.source_name}")
            inputs = (
                self.session.query(ProductInput)
                .join(Product, Product.id == ProductInput.product_id)
                .join(Source, Source.id == ProductInput.source_id)
                .filter(Source.name == self.source_name)
                .all()
            )
            return {i.product.name.strip().lower(): i.id for i in inputs}
        except Exception as e:
            logger.error(f"Error fetching product input mapping: {str(e)}")
            return {}

    def save(self) -> bool:
        success_count = 0
        skip_count = 0
        fail_count = 0

        try:
            mapping = self.get_product_input_map()
            source_obj = self.get_or_create_source(self.source_name)
            if not source_obj:
                logger.error(f"Source '{self.source_name}' not found.")
                return False

            source_id = source_obj.id

            for _, row in self.df.iterrows():
                try:
                    product_name = str(row.get("product", "")).strip().lower()
                    price_value = row.get("price_value") or row.get("Price")
                    price_date = pd.to_datetime(row.get("price_date") or row.get("Date"))
                    category = row.get("product_category")

                    config_id = mapping.get(product_name)
                    if not config_id:
                        logger.warning(f"Skipping unknown product: {product_name}")
                        skip_count += 1
                        continue

                    record = PriceRaw(
                        source_id=source_id,
                        product_config_id=config_id,
                        price_date=price_date,
                        price_value=float(str(price_value).replace(",", "").replace(" ", "")),
                        product_category=category,
                        last_update=datetime.utcnow()
                    )

                    self.session.add(record)
                    self.session.commit()
                    success_count += 1

                except IntegrityError as ie:
                    self.session.rollback()
                    logger.warning(f"Duplicate skipped: {product_name} on {price_date.date()} (src={source_id}, cfg={config_id})")
                    skip_count += 1

                except Exception as e:
                    self.session.rollback()
                    logger.error(f"Failed row: {product_name} on {price_date} → {str(e)}")
                    fail_count += 1

            logger.info(f"Inserted: {success_count}, Duplicates Skipped: {skip_count}, Failed: {fail_count}")
            return True if success_count else False

        except Exception as e:
            logger.error(f"Total failure: {str(e)}")
            self.session.rollback()
            return False

        finally:
            self.session.close()