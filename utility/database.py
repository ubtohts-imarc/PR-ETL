import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from decouple import config
from db.models.base import Base

# Read DB URL
PR_DATABASE_URL = config("PR_DATABASE_URL")
if not PR_DATABASE_URL:
    raise ValueError("PR_DATABASE_URL environment variable not set")

# Create engine
engine = create_engine(
    PR_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

Base.metadata.create_all(bind=engine)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency for FastAPI
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
