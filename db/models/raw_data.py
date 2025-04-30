from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

# To store all the raw data for products
# i.e: (sunsirs, Hydrofluoric acid, 2025-04-01, 1000.0000, 1, Non-ferrous metals)
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

    product_input = relationship("db.models.input.ProductInput", backref="raw_prices", primaryjoin="db.models.input.ProductInput.id == PriceRaw.product_config_id")
    source = relationship("db.models.metadata.Source", primaryjoin="db.models.metadata.Source.id == PriceRaw.source_id")
    standard_price = relationship("db.models.transformed.PriceStandardized", backref="raw_data", uselist=False, primaryjoin="PriceRaw.id == db.models.transformed.PriceStandardized.raw_data_id")

    def __str__(self):
        return f"{self.product_input.product.name} - {self.price_date} - {self.price_value}"
    
    def __repr__(self):
        return f"<PriceRaw(id={self.id}, product_config_id={self.product_config_id}, price_date={self.price_date}, price_value={self.price_value})>"