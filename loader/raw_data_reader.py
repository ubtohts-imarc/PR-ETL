from sqlalchemy.orm import Session, joinedload
from models.raw_data import PriceRaw
from models.input import ProductInput
from utility.logger import get_logger
from loader.base_loader import BaseLoader
import pandas as pd

logger = get_logger()


class RawPriceFetcher(BaseLoader):
    def __init__(self, website_name: str):
        super().__init__()
        self.website_name = website_name.strip().lower()

    def fetch(self) -> pd.DataFrame:
        try:
            logger.info(f"Fetching data for website: {self.website_name}")

            # Fetch PriceRaw records joined with ProductInput
            query = (
                self.session.query(PriceRaw, ProductInput)
                .join(ProductInput, ProductInput.id == PriceRaw.product_config_id)
                .filter(PriceRaw.source_name == self.website_name)
                # .options(joinedload(PriceRaw.product_config))  # Optional, eager loading
            )

            results = query.all()
            logger.info(f"Fetched {len(results)} records.")

            # Convert to list of dicts
            data = []
            for price_raw, input_config in results:
                data.append({
                    "price_date": price_raw.price_date,
                    "price_value": float(price_raw.price_value),
                    "product_category": price_raw.product_category,
                    "product_config_id": price_raw.product_config_id,
                    "expected_currency_code": input_config.expected_currency_code,
                    "expected_unit_code": input_config.expected_unit_code,
                    "expected_quantity": float(input_config.expected_quantity),
                    "location_id": input_config.location_id,
                })

            df = pd.DataFrame(data)
            logger.info(f"Created DataFrame with shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error fetching price raw data: {str(e)}")
            return pd.DataFrame()

        finally:
            self.session.close()
