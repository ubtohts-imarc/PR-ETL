from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from models.base import Base

class PriceRaw(Base):
    __tablename__ = "products_raw_data"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "price_date",
            "product_config_id",
            name="uq_raw_source_product_config"
        ),
        {"schema": "raw_data"},
    )

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("metadata.source.id"), nullable=False)
    product_config_id = Column(Integer, ForeignKey("input.products_input_data.id"), nullable=False)
    price_date = Column(Date, nullable=False)
    price_value = Column(DECIMAL(18, 4), nullable=False)
    product_category = Column(String(100), nullable=True)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    product_input = relationship("models.input.ProductInput", backref="raw_prices", primaryjoin="models.input.ProductInput.id == PriceRaw.product_config_id")
    source = relationship("models.metadata.Source", primaryjoin="models.metadata.Source.id == PriceRaw.source_id")
    standard_price = relationship("models.transformed.PriceStandardized", backref="raw_data", uselist=False, primaryjoin="PriceRaw.id == models.transformed.PriceStandardized.raw_data_id")