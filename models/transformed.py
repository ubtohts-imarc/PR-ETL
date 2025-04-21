from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from models.base import Base

class PriceStandardized(Base):
    __tablename__ = "products_transformed_data"
    __table_args__ = (
        UniqueConstraint(
            "source_name", 
            "product_id", 
            "location_id", 
            "source_date", 
            name="uq_std_source_product_location_date"
        ),
        {"schema": "transformed"},
    )

    id = Column(Integer, primary_key=True)
    source_name = Column(String(100), nullable=False)
    product_id = Column(Integer, ForeignKey("metadata.product.id"), nullable=False)
    location_id = Column(String(50), ForeignKey("metadata.location.name"), nullable=True)
    quantity = Column(DECIMAL(18, 6))
    unit_code = Column(String(50), ForeignKey("metadata.unit.code"))
    price_usd = Column(DECIMAL(18, 6), nullable=False)
    source_date = Column(Date, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
