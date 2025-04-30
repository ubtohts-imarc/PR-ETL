from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from utility.database import SessionLocal
from api_server.app.data.service import get_all_input_data

router = APIRouter()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/input-data")
async def fetch_input_data(db: Session = Depends(get_db)):
    data = get_all_input_data(db)
    return data
