from sqlalchemy import Column, Integer, Float, String, ForeignKey, DateTime, Date, Text, func, UniqueConstraint
from sqlalchemy.orm import relationship
from db.models.base import Base

# To store all the websites name
# i.e: ('sunsirs').
class Source(Base):
    __tablename__ = "source"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __str__(self):
        return self.name  # or return f"{self.code} - {self.name}"

    def __repr__(self):
        return f"<Source(id={self.id}, name={self.name})>"

# To store all the commodity/products name
# i.e: (USD, United States Dollar), (EUR, Euro).
class Product(Base):
    __tablename__ = "product"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return f"<Product(id={self.id}, name={self.name})>"

# To store all the locations with its region name.
# i.e: (United States, North America), (India, Asia)
class Location(Base):
    __tablename__ = "location"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    region = Column(String(100))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return f"<Location(id={self.id}, name={self.name})>"

# To store all the units name.
# i.e: (kg, kilogram (mass)), (tonne, metric ton (mass))
class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    description = Column(String(255))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __str__(self):
        return self.code
    
    def __repr__(self):
        return f"<Unit(id={self.id}, code={self.code})>"

# To store all the currencies name.
# i.e: (USD, United States Dollar), (EUR, Euro).
class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __str__(self):
        return self.code
    
    def __repr__(self):
        return f"<Currency(id={self.id}, code={self.code})>"

# To store all the currency conversion rates.
# i.e: (sunsirs, 2025-01-01, 2025-02-01, 'https://sunsris.com/{replacedatehere}').
class WebConfig(Base):
    __tablename__ = "web_config"
    __table_args__ = {"schema": "metadata"}

    source_id = Column(Integer, ForeignKey("metadata.source.id"), primary_key=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    base_url = Column(Text, nullable=True)

    source = relationship("Source", backref="web_configs")

    def __str__(self):
        return f"{self.source.name} - {self.start_date} to {self.end_date}"
    
    def __repr__(self):
        return f"<WebConfig(source_id={self.source_id}, start_date={self.start_date}, end_date={self.end_date}, base_url={self.base_url})>"

# To store all the unit conversion rates.
# i.e: (kg, tonne, mass, 1000.0).
class UnitConversion(Base):
    __tablename__ = "unit_conversion"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    unit_id = Column(Integer, ForeignKey("metadata.unit.id"), nullable=False)
    base_unit_id = Column(Integer, ForeignKey("metadata.unit.id"), nullable=False)
    category = Column(String(50), nullable=False)
    conversion_factor = Column(Float, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    unit = relationship("Unit", foreign_keys=[unit_id])
    base_unit = relationship("Unit", foreign_keys=[base_unit_id])

    def __str__(self):
        return f"{self.unit.code} to {self.base_unit.code} - {self.conversion_factor}"
    
    def __repr__(self):
        return f"<UnitConversion(id={self.id}, unit_id={self.unit_id}, base_unit_id={self.base_unit_id}, conversion_factor={self.conversion_factor})>"
    

class ExchangeRate(Base):
    __tablename__ = "exchange_rate"
    __table_args__ = (
        UniqueConstraint(
            'date',
            'from_currency_id',
            'to_currency_id',
            name='uq_exchange_date_pair'
        ),
        {"schema": "metadata"}
    )

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    from_currency_id = Column(Integer, ForeignKey("metadata.currency.id"), nullable=False)
    to_currency_id = Column(Integer, ForeignKey("metadata.currency.id"), nullable=False)
    average_rate = Column(Float, nullable=False)
    min_rate = Column(Float, nullable=True)
    max_rate = Column(Float, nullable=True)
    last_update = Column(Date, server_default=func.now(), onupdate=func.now())

    from_currency = relationship("Currency", foreign_keys=[from_currency_id])
    to_currency = relationship("Currency", foreign_keys=[to_currency_id])

    def __repr__(self):
        return f"<ExchangeRate({self.date} | {self.from_currency.code} → {self.to_currency.code} = {self.average_rate})>"