from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from models.base import Base

class Product(Base):
    __tablename__ = "product"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Location(Base):
    __tablename__ = "location"
    __table_args__ = {"schema": "metadata"}

    name = Column(String(100), primary_key=True)
    region = Column(String(100))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())


class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = {"schema": "metadata"}

    code = Column(String(50), primary_key=True)
    description = Column(String(255))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())


class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = {"schema": "metadata"}

    code = Column(String(10), primary_key=True)
    name = Column(String(50))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())
