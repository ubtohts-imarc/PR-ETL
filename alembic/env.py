import os
import sys
from logging.config import fileConfig
from models.base import Base
from alembic import context
from sqlalchemy import engine_from_config, pool, create_engine, MetaData, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the root of your project to the path (so Python can import models)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your models here
from models.metadata import Base as ReferenceBase
from models.input import Base as InputBase
from models.raw_data import Base as ScrapedBase
from models.transformed import Base as TransformedBase

# Alembic Config object
config = context.config
config.set_main_option('sqlalchemy.url', os.getenv("DATABASE_URL"))

# Log the database connection URL (for debugging purposes)
print(f"Connecting to database at {os.getenv('DATABASE_URL')}")

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
        )

        with context.begin_transaction():
            context.run_migrations()

# Determine whether to run migrations offline or online
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()