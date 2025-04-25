from sqlalchemy import Column, Integer, String, Date, DECIMAL, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

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

    product = relationship("models.metadata.Product", primaryjoin="models.metadata.Product.id == PriceStandardized.product_id")
    source = relationship("models.metadata.Source", primaryjoin="models.metadata.Source.id == PriceStandardized.source_id")
    location = relationship("models.metadata.Location", primaryjoin="models.metadata.Location.id == PriceStandardized.location_id")
    unit = relationship("models.metadata.Unit", primaryjoin="models.metadata.Unit.id == PriceStandardized.unit_id")