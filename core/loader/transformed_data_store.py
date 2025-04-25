from sqlalchemy.exc import IntegrityError
from db.models.transformed import PriceStandardized
from core.loader.base_loader import BaseLoader
from datetime import datetime
import pandas as pd
from utility.logger import get_logger

logger = get_logger()

class StandardizedPriceWriter(BaseLoader):
    def __init__(self, df: pd.DataFrame, source_name: str):
        super().__init__()
        self.df = df
        self.source_name = source_name.strip().lower()

    def save(self):
        success_count = 0
        skip_count = 0
        fail_count = 0
        try:
            for _, row in self.df.iterrows():
                try:
                    source_obj = self.get_or_create_source(self.source_name)
                    # logger.info(f"Source object: {source_obj}")
                    if not source_obj:
                        logger.warning(f"Source '{self.source_name}' not found in metadata.")
                        continue

                    unit_code = str(row.get("final_unit_code")).strip().lower()
                    unit_id = self.get_unit_id_by_code(unit_code)
                    if not unit_id:
                        logger.warning(f"Skipping row due to unknown unit code: {unit_code}")
                        continue

                    record = PriceStandardized(
                        source_id=source_obj.id,
                        product_id=int(row.get("product_config_id")),
                        location_id=int(row.get("location_id")) if row.get("location_id") else None,
                        quantity=float(row.get("expected_quantity")) if row.get("expected_quantity") else None,
                        unit_id=int(unit_id),
                        price_usd=float(row.get("price_value")),
                        source_date=pd.to_datetime(row.get("price_date")).date(),
                        raw_data_id=int(row.get("id")) if row.get("id") else None,
                        last_update=datetime.utcnow()
                    )
                    self.session.add(record)
                    self.session.commit()
                    success_count += 1

                except IntegrityError as ie:
                    self.session.rollback()
                    logger.warning(f"Duplicate skipped: {source_obj} on {row.get("price_date")}")
                    skip_count += 1

                except Exception as e:
                    self.session.rollback()
                    logger.error(f"Failed row: {source_obj} on {row.get("price_date")} → {str(e)}")
                    fail_count += 1

            logger.info(f"Inserted: {success_count}, Duplicates Skipped: {skip_count}, Failed: {fail_count}")
            return True if success_count else False

        except Exception as e:
            logger.error(f"Error saving standardized prices: {str(e)}")
            self.session.rollback()
        finally:
            self.close_session()
