import os
import sys
from logging.config import fileConfig

from decouple import config as env_config
from sqlalchemy import create_engine, engine_from_config, pool, text
from alembic import context

from db.models.base import Base

# Add the root of your project to the path (so Python can import models)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
print(f"Current working directory: {os.getcwd()}")
print(f"\nPython path: {sys.path}")

print(f"\nLooking for .env file in directory: {os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))}")

print(f"Loading .env file from: {os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))}")
# Check if the .env file exists
env_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
if os.path.exists(env_file_path):
    print(f".env file found at {env_file_path}")
else:
    print(f".env file not found at {env_file_path}")

# Import your models here
from db.models.metadata import Source, Currency, Unit, Location, Product
from db.models.input import ProductInput
from db.models.raw_data import PriceRaw
from db.models.transformed import PriceStandardized

# Alembic Config object
config = context.config
config.set_main_option('sqlalchemy.url', env_config("DATABASE_URL"))

# Log the database connection URL (for debugging purposes)
print(f"Connecting to AIRFLOW database at {env_config('AIRFLOW_DATABASE_URL')}")
print(f"Connecting to ETL database at {env_config('DATABASE_URL')}")

# Set up logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata  # All models are registered here

def create_schemas():
    """
    Create required schemas in the database if they do not exist.
    """
    engine = create_engine(context.config.get_main_option("sqlalchemy.url"))
    with engine.connect() as connection:
        # Add schema creation logic here
        schemas = ["input", "metadata", "raw_data", "transformed"]
        for schema in schemas:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

# Call the function to create schemas before running migrations
create_schemas()

def run_migrations_offline():
    """
    Run migrations in 'offline' mode.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """
    Run migrations in 'online' mode.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()

# Determine whether to run migrations offline or online
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()