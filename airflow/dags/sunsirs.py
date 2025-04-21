from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from utility.logger import get_logger
from websites.sunsirs import SunsirsExtractor, SunsirsTransformer
from datetime import datetime

logger = get_logger()

def _extract():
    try:
        extractor = SunsirsExtractor()
        extracted_data = extractor.extract()
        logger.info(f"Extracted data: {extracted_data}")
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")

def _transform():
    try:
        transform = SunsirsTransformer()
        extracted_data = transform.transform()
        logger.info(f"Transformed data: {extracted_data}")
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")

with DAG(
    dag_id="sunsirs_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sunsirs"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        provide_context=True,
    )

    extract >> transform

with DAG(
    dag_id="sunsirs_extract",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sunsirs"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        provide_context=True,
    )

    extract

with DAG(
    dag_id="sunsirs_transform",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sunsirs"],
) as dag:

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        provide_context=True,
    )

    transform