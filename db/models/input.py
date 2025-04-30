from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, Text, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

# To store all the input data for products
# i.e: (sunsirs, None, Hydrofluoric acid, False, INR, tonne, 1.0000, India)
class ProductInput(Base):
    __tablename__ = "products_input_data"
    __table_args__ =(
        UniqueConstraint(
            "source_id",
            "product_id",
            "input_unit_id",
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
    input_currency_id = Column(Integer, ForeignKey("metadata.currency.id"), nullable=False)
    input_unit_id = Column(Integer, ForeignKey("metadata.unit.id"), nullable=False)
    expected_unit_id = Column(Integer, ForeignKey("metadata.unit.id"), nullable=False)
    input_quantity = Column(DECIMAL(18, 4), nullable=False)
    location_id = Column(Integer, ForeignKey("metadata.location.id"), nullable=True)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    product = relationship("db.models.metadata.Product", backref="input_configs", primaryjoin="db.models.metadata.Product.id == ProductInput.product_id")
    source = relationship("db.models.metadata.Source", primaryjoin="db.models.metadata.Source.id == ProductInput.source_id")
    currency = relationship("db.models.metadata.Currency", primaryjoin="db.models.metadata.Currency.id == ProductInput.input_currency_id")
    unit = relationship("db.models.metadata.Unit", primaryjoin="db.models.metadata.Unit.id == ProductInput.input_unit_id")
    location = relationship("db.models.metadata.Location", primaryjoin="db.models.metadata.Location.id == ProductInput.location_id")

    def __str__(self):
        return f"{self.product.name} - {self.location.name} - {self.unit.code} - {self.input_quantity}"
    
    def __repr__(self):
        return f"<ProductInput(id={self.id}, product_id={self.product_id}, location_id={self.location_id}, input_unit_id={self.input_unit_id})>"