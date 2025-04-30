from fastapi import FastAPI
from sqladmin import Admin
from sqladmin import ModelView
from db.models.input import ProductInput
from db.models.raw_data import PriceRaw
from db.models.transformed import PriceStandardized
from db.models.metadata import Source, Product, Location, Unit, Currency, WebConfig
from utility.database import engine

print(f"Engine: {engine}")  # Add this in your setup_admin function to verify engine
print(f"Product Input Table: {ProductInput.__table__}")  # Check if the table is mapped properly
print(f"Product Input Table Columns: {ProductInput.__table__.columns}")  # Check if the table is mapped properly

def setup_admin(app: FastAPI):
    admin = Admin(app, engine)

    class ProductInputAdmin(ModelView, model=ProductInput):
        column_list = [
            "id",
            "source.name",
            "product.name",
            "input_quantity",
            "currency.code",
            "unit.code",
            "location.name",
            "last_update"
        ]

    class PriceRawAdmin(ModelView, model=PriceRaw):
        column_list = [
            "id",
            "source.name",
            "price_date",
            "product_input.product.name",
            "product_input.location.name",
            "price_value",
            "product_category",
            "last_update"
        ]

    class PriceStandardizedAdmin(ModelView, model=PriceStandardized):
        column_list = [
            "id",
            "source.name",
            "product.name",
            "location.name",
            "unit.code",
            "price_usd",
            "source_date",
            "last_update"
        ]
    
    class SourceAdmin(ModelView, model=Source):
        column_list = [
            "id",
            "name",
            "last_update"
        ]
    
    class ProductAdmin(ModelView, model=Product):
        column_list = [
            "id",
            "name",
            "last_update"
        ]
    
    class LocationAdmin(ModelView, model=Location):
        column_list = [
            "id",
            "name",
            "region",
            "last_update"
        ]
    
    class UnitAdmin(ModelView, model=Unit):
        column_list = [
            "id",
            "code",
            "description",
            "last_update"
        ]
    
    class CurrencyAdmin(ModelView, model=Currency):
        column_list = [
            "id",
            "code",
            "name",
            "last_update"
        ]
    
    class WebConfigAdmin(ModelView, model=WebConfig):
        column_list = [
            "source.name",
            "start_date",
            "end_date",
            "base_url"
        ]


    admin.add_view(ProductInputAdmin)
    admin.add_view(PriceRawAdmin)
    admin.add_view(PriceStandardizedAdmin)
    admin.add_view(SourceAdmin)
    admin.add_view(ProductAdmin)
    admin.add_view(LocationAdmin)
    admin.add_view(UnitAdmin)
    admin.add_view(CurrencyAdmin)
    admin.add_view(WebConfigAdmin)
