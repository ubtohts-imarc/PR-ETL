from sqlalchemy.orm import Session
from db.models.input import ProductInput

def get_all_input_data(db: Session):
    return db.query(ProductInput).all()
