from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from models.base import Base

class PriceRaw(Base):
    __tablename__ = "products_raw_data"
    __table_args__ = (
        UniqueConstraint(
            "source_name",
            "price_date",
            "product_config_id", 
            name="uq_raw_website_date_config"
        ),
        {"schema": "raw_data"},
    )

    id = Column(Integer, primary_key=True)
    source_name = Column(String(100), nullable=False)
    price_date = Column(Date, nullable=False)
    price_value = Column(DECIMAL(18, 6), nullable=False)
    product_category = Column(String(100), nullable=True)
    product_config_id = Column(Integer, ForeignKey("input.products_input_data.id"), nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
