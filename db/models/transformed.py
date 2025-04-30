from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

# To store all the data for products
# i.e: (sunsirs, Hydrofluoric acid, 2025-04-01, 1000.0000, 1, Non-ferrous metals)
class PriceStandardized(Base):
    __tablename__ = "products_standard_data"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "product_id",
            "location_id",
            "source_date",
            name="uq_std_source_product_location_date"
        ),
        {"schema": "transformed"},
    )

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("metadata.source.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("metadata.product.id"), nullable=False)
    location_id = Column(Integer, ForeignKey("metadata.location.id"), nullable=True)
    quantity = Column(DECIMAL(18, 4))
    unit_id = Column(Integer, ForeignKey("metadata.unit.id"))
    price_usd = Column(DECIMAL(18, 4), nullable=False)
    source_date = Column(Date, nullable=False)
    raw_data_id = Column(Integer, ForeignKey("raw_data.products_raw_data.id"), nullable=True)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    product = relationship("db.models.metadata.Product", primaryjoin="db.models.metadata.Product.id == PriceStandardized.product_id")
    source = relationship("db.models.metadata.Source", primaryjoin="db.models.metadata.Source.id == PriceStandardized.source_id")
    location = relationship("db.models.metadata.Location", primaryjoin="db.models.metadata.Location.id == PriceStandardized.location_id")
    unit = relationship("db.models.metadata.Unit", primaryjoin="db.models.metadata.Unit.id == PriceStandardized.unit_id")

    def __str__(self):
        return f"{self.product.name} - {self.location.name} - {self.unit.code} - {self.price_usd}"
    
    def __repr__(self):
        return f"<PriceStandardized(id={self.id}, product_id={self.product_id}, location_id={self.location_id}, price_usd={self.price_usd})>"