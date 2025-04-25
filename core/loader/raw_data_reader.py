from sqlalchemy.orm import joinedload
from db.models.raw_data import PriceRaw
from db.models.metadata import Source
from db.models.input import ProductInput
from utility.logger import get_logger
from core.loader.base_loader import BaseLoader
import pandas as pd

logger = get_logger()


class RawPriceFetcher(BaseLoader):
    def __init__(self, website_name: str):
        super().__init__()
        self.source_name = website_name.strip().lower()

    def fetch(self) -> pd.DataFrame:
        try:
            logger.info(f"Fetching data for website: {self.source_name}")

            # Full join with relationships for eager load of unit/currency
            query = (
                self.session.query(PriceRaw)
                .join(ProductInput, ProductInput.id == PriceRaw.product_config_id)
                .join(Source, Source.id == PriceRaw.source_id)
                .options(
                    joinedload(PriceRaw.product_input).joinedload(ProductInput.currency),
                    joinedload(PriceRaw.product_input).joinedload(ProductInput.unit)
                )
                .filter(Source.name == self.source_name)
            )

            results = query.all()
            logger.info(f"Fetched {len(results)} records.")

            data = []
            for row in results:
                input_config = row.product_input
                data.append({
                    "id": row.id,
                    "price_date": row.price_date,
                    "price_value": float(row.price_value),
                    "product_category": row.product_category,
                    "product_config_id": row.product_config_id,
                    "expected_currency_id": input_config.expected_currency_id,
                    "expected_currency_code": input_config.currency.code if input_config.currency else None,
                    "expected_unit_id": input_config.expected_unit_id,
                    "expected_unit_code": input_config.unit.code if input_config.unit else None,
                    "expected_quantity": float(input_config.expected_quantity),
                    "location_id": input_config.location_id,
                })

            df = pd.DataFrame(data)
            logger.info(f"Created DataFrame with shape: {df.shape}")
            logger.info(f"Created DataFrame with shape: {df.head(5)}")
            return df

        except Exception as e:
            logger.error(f"Error fetching price raw data: {str(e)}")
            return pd.DataFrame()

        finally:
            self.session.close()
