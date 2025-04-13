from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

# Database URL (Modify as per your environment)
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# Create the database engine
engine = create_engine(DATABASE_URL)

# Create a configured session class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

# Dependency function to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db  # Provides a session to the API endpoint
    finally:
        db.close()  # Closes the session after request is handled
