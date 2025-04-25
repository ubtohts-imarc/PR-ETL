from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from sqlalchemy.exc import NoResultFound
from db.models.metadata import Source, Unit
import os
from utility.logger import get_logger

logger = get_logger()

class BaseLoader:
    def __init__(self):
        load_dotenv()
        self.logger = get_logger()

        # Load DB config and initialize engine
        database_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.session: Session = self.SessionLocal()

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