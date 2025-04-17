from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from utility.logger import get_logger
from websites.sunsirs import SunsirsExtractor
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

logger = get_logger()

DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def _extract(**kwargs):
    data = SunsirsExtractor().extract()
    df = data.get("extracted_data")

    if df is None or df.empty:
        raise ValueError("No data extracted.")

    # Convert datetime columns to string for JSON serialization
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)  # or df[col].dt.strftime("%Y-%m-%d")

    # Push cleaned data to XCom
    kwargs['ti'].xcom_push(key="data", value=df.to_dict(orient="records"))
    logger.info("Data pushed to XCom.")


def _load(**kwargs):
    records = kwargs['ti'].xcom_pull(task_ids="extract", key="data")

    if not records:
        raise ValueError("No data received from extract.")

    df = pd.DataFrame(records)
    logger.info(f"Loading {len(df)} rows into database...")

    engine = create_engine(DATABASE_URL)
    with engine.begin() as connection:
        df.to_sql("sunsirs_data", con=connection, if_exists="append", index=False)
        logger.info("Data successfully loaded into database.")

with DAG(
    dag_id="sunsirs_etl",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sunsirs"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load,
        provide_context=True,
    )

    extract >> load
