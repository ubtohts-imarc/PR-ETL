from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from datetime import datetime
import pandas as pd
from db.models.metadata import Source, Unit, Currency, ExchangeRate
from utility.logger import get_logger
from utility.database import engine, SessionLocal

logger = get_logger()

class BaseLoader:
    def __init__(self):
        self.logger = get_logger()
        self.engine = engine
        self.SessionLocal = SessionLocal
        self.session = self.SessionLocal()

    def close_session(self):
        try:
            self.session.close()
        except Exception as e:
            self.logger.error(f"Error closing session: {str(e)}")

    def get_or_create_source(self, name):
        try:
            return self.session.query(Source).filter_by(name=name).one()
        except NoResultFound:
            source = Source(name=name)
            self.session.add(source)
            self.session.flush()  # To get the ID without committing
            return source

    def get_unit_id_by_code(self, unit_code: str):
        try:
            unit = self.session.query(Unit).filter(Unit.code == unit_code).first()
            if not unit:
                logger.warning(f"Unit not found for code: '{unit_code}'")
                return None
            # logger.info(f"Unit found for code '{unit_code}': {unit.id}")
            return unit.id
        except Exception as e:
            logger.error(f"Error fetching unit for code '{unit_code}': {str(e)}")
            return None
        
    
    def insert_currencies_data(self, currency_dict: dict) -> bool:
        for code, name in currency_dict.items():
            try:
                # Try to fetch existing currency
                currency = self.session.query(Currency).filter_by(code=code).first()

                if currency:
                    # Update name if it has changed
                    old_name = currency.name
                    if old_name != name:
                        currency.name = name
                        self.logger.info(f"Updated currency: {code}, name from '{old_name}' to '{currency.name}'")
                else:
                    # Insert new currency
                    currency = Currency(code=code, name=name)
                    self.session.add(currency)
                    self.logger.info(f"Inserted currency: {code}")

            except SQLAlchemyError as e:
                self.logger.error(f"Error processing currency {code}: {e}")
                self.session.rollback()

        try:
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.logger.error(f"Error during commit: {e}")
            self.session.rollback()
            return False
        
    def store_exchange_rate_dataframe(self, df: pd.DataFrame, from_code: str, to_code: str) -> bool:
        """
        Store parsed exchange rate data from a pandas DataFrame into the database.

        :param df: DataFrame containing exchange rate data
        :param from_code: source currency code (e.g., 'USD')
        :param to_code: target currency code (e.g., 'INR')
        """
        try:
            from_currency = self.session.query(Currency).filter(Currency.code == from_code).first()
            to_currency = self.session.query(Currency).filter(Currency.code == to_code).first()

            if not from_currency or not to_currency:
                self.logger.error(f"Currency not found in DB: {from_code} or {to_code}")
                return

            for _, row in df.iterrows():
                try:
                    # Parse and clean values
                    date_str = str(row['Date']).split()[1:]  # Drop weekday name
                    date_obj = datetime.strptime(' '.join(date_str), "%d %B %Y").date()

                    try:
                        average = float(row.get('Average')) if not pd.isna(row.get('Average')) else None
                        min_val = float(row.get('Min')) if not pd.isna(row.get('Min')) else None
                        max_val = float(row.get('Max')) if not pd.isna(row.get('Max')) else None
                    except Exception as e:
                        self.logger.warning(f"Float conversion error on date {row['Date']}: {e}")
                        continue

                    if pd.isna(average):
                        self.logger.warning(f"Skipping invalid average for date {date_obj} [{from_code} → {to_code}]")
                        continue

                    # Check if record exists
                    existing = (
                        self.session.query(ExchangeRate)
                        .filter_by(date=date_obj, from_currency_id=from_currency.id, to_currency_id=to_currency.id)
                        .first()
                    )

                    if existing:
                        existing.average_rate = average
                        existing.min_rate = min_val
                        existing.max_rate = max_val
                        self.logger.info(f"Updated rate for {from_code} → {to_code} on {date_obj}")
                    else:
                        new_rate = ExchangeRate(
                            date=date_obj,
                            from_currency_id=from_currency.id,
                            to_currency_id=to_currency.id,
                            average_rate=average,
                            min_rate=min_val,
                            max_rate=max_val
                        )
                        self.session.add(new_rate)
                        self.logger.info(f"Inserted new rate for {from_code} → {to_code} on {date_obj}")
                except Exception as e:
                    self.logger.error(f"Row-level error for date [{row['Date']}]: {str(e)}")
                    continue
        
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.logger.error(f"Database error: {str(e)}")
            self.session.rollback()
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            self.session.rollback()
            return False