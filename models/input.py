from sqlalchemy import Column, Integer, String, DECIMAL, Date, Text, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship
from models.base import Base


class ProductInput(Base):
    __tablename__ = "products_input_data"
    __table_args__ =(
        UniqueConstraint(
            "source_name",
            "product_id",
            "expected_unit_code",
            "location_id",
            name="uq_source_product_unit_location"
        ),
        {"schema": "input"}
    )

    id = Column(Integer, primary_key=True)
    source_name = Column(String(100), nullable=False)
    source_url = Column(Text, nullable=True)
    product_id = Column(Integer, ForeignKey("metadata.product.id"), nullable=False)
    expected_currency_code = Column(String(10), ForeignKey("metadata.currency.code"), nullable=False)
    expected_unit_code = Column(String(50), ForeignKey("metadata.unit.code"), nullable=False)
    expected_quantity = Column(DECIMAL(18, 6), nullable=False)
    location_id = Column(String(50), ForeignKey("metadata.location.name"), nullable=True)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
    product = relationship(
        "models.metadata.Product",
        backref="input_configs",
        primaryjoin="models.metadata.Product.id == ProductInput.product_id"
    )