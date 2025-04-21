from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
import os
from utility.logger import get_logger

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
