from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, Text, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

class ProductInput(Base):
    __tablename__ = "products_input_data"
    __table_args__ =(
        UniqueConstraint(
            "source_id",
            "product_id",
            "expected_unit_id",
            "location_id",
            name="uq_source_product_unit_location"
        ),
        {"schema": "input"}
    )

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("metadata.source.id"), nullable=False)
    source_url = Column(Text, nullable=True)
    product_id = Column(Integer, ForeignKey("metadata.product.id"), nullable=False)
    upload_on_pr = Column(Boolean, default=False)
    expected_currency_id = Column(Integer, ForeignKey("metadata.currency.id"), nullable=False)
    expected_unit_id = Column(Integer, ForeignKey("metadata.unit.id"), nullable=False)
    expected_quantity = Column(DECIMAL(18, 4), nullable=False)
    location_id = Column(Integer, ForeignKey("metadata.location.id"), nullable=True)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    product = relationship("models.metadata.Product", backref="input_configs", primaryjoin="models.metadata.Product.id == ProductInput.product_id")
    source = relationship("models.metadata.Source", primaryjoin="models.metadata.Source.id == ProductInput.source_id")
    currency = relationship("models.metadata.Currency", primaryjoin="models.metadata.Currency.id == ProductInput.expected_currency_id")
    unit = relationship("models.metadata.Unit", primaryjoin="models.metadata.Unit.id == ProductInput.expected_unit_id")
    location = relationship("models.metadata.Location", primaryjoin="models.metadata.Location.id == ProductInput.location_id")