from sqlalchemy import Column, Integer, String, DateTime, func
from models.base import Base

class Source(Base):
    __tablename__ = "source"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Product(Base):
    __tablename__ = "product"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Location(Base):
    __tablename__ = "location"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    region = Column(String(100))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    description = Column(String(255))
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = {"schema": "metadata"}

    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, nullable=False)
    name = Column(String(50), nullable=False)
    last_update = Column(DateTime, server_default=func.now(), onupdate=func.now())