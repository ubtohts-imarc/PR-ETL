import pandas as pd
from sqlalchemy.exc import IntegrityError

from db.models.input import ProductInput
from db.models.metadata import Currency, Location, Product, Source, Unit
from etl_pipeline.core.loader.base_loader import BaseLoader
from utility.logger import get_logger

logger = get_logger()

class ProductInputExcelLoader(BaseLoader):
    def __init__(self, excel_path: str):
        super().__init__()
        self.df = pd.read_excel(excel_path)

    def get_id_by_name(self, model, field, value):
        """Generic method to fetch ID from metadata tables"""
        record = self.session.query(model).filter(field == value).first()
        if record:
            return record.id
        else:
            logger.warning(f"No match found for '{value}' in {model.__name__}, creating it.")
            try:
                # Dynamically create object based on model
                if model.__name__ == "Product":
                    new_obj = model(name=value)
                # elif model.__name__ == "Location":
                #     new_obj = model(name=value)
                # elif model.__name__ == "Currency":
                #     new_obj = model(code=value, name=value)
                # elif model.__name__ == "Unit":
                #     new_obj = model(code=value, description=value)
                elif model.__name__ == "Source":
                    new_obj = model(name=value)
                else:
                    logger.error(f"Unknown model: {model.__name__}")
                    return None

                self.session.add(new_obj)
                self.session.commit()
                return new_obj.id
            except Exception as e:
                self.session.rollback()
                logger.error(f"Failed to create {model.__name__} with value '{value}': {e}")
                return None

    def load(self):
        inserted_count = 0
        for _, row in self.df.iterrows():
            try:
                product_name = str(row.get("Product")).strip()
                location_name = str(row.get("Location")).strip()
                source_name = str(row.get("Source")).strip()
                currency_code = str(row.get("Current Currency")).strip()
                unit_code = str(row.get("Current Unit")).strip()
                quantity = float(row.get("Current Quantity"))

                # Lookup IDs
                product_id = self.get_id_by_name(Product, Product.name, product_name)
                location_id = self.get_id_by_name(Location, Location.name, location_name)
                currency_id = self.get_id_by_name(Currency, Currency.code, currency_code)
                unit_id = self.get_id_by_name(Unit, Unit.code, unit_code)
                source_id = self.get_id_by_name(Source, Source.name, source_name)

                if None in [product_id, location_id, currency_id, unit_id, source_id]:
                    logger.warning(f"Skipping row due to missing metadata: {row.to_dict()}")
                    continue

                # Prepare and insert ProductInput
                input_obj = ProductInput(
                    product_id=product_id,
                    location_id=location_id,
                    expected_unit_id=unit_id,
                    expected_currency_id=currency_id,
                    expected_quantity=quantity,
                    source_id=source_id,
                    upload_on_pr=str(row.get("Upload on PR")).strip().lower() == "true"
                )

                self.session.add(input_obj)
                self.session.commit()
                inserted_count += 1
                logger.info(f"Inserted ProductInput for: {product_name}")

            except IntegrityError:
                self.session.rollback()
                logger.info(f"Duplicate found, skipping existing entry for: {product_name} - {location_name}")
            except Exception as e:
                self.session.rollback()
                logger.error(f"Error processing row: {row.to_dict()}, Error: {str(e)}")

        logger.info(f"Finished loading. Total inserted: {inserted_count}")
        self.close_session()


# Example usage:
if __name__ == "__main__":
    loader = ProductInputExcelLoader(r"C:\Code\Ubtohts-Imarc\Sunsirs_Input.xlsx")
    loader.load()
