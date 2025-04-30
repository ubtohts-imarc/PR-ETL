from datetime import datetime 
import requests
import pandas as pd
from abc import ABC
from typing import Any, Dict, Optional
from sqlalchemy import func
from sqlalchemy.orm import Session
from requests.exceptions import HTTPError, Timeout, RequestException
from tenacity import retry, stop_after_attempt, wait_exponential

from db.models.metadata import Source, WebConfig
from db.models.transformed import PriceStandardized
from etl_pipeline.core.extract.table_extractor import TableExtractor
from utility.logger import get_logger
from utility.database import engine, SessionLocal
from swiftshadow.classes import ProxyInterface

logger = get_logger()

class BaseExtractor(ABC):
    def __init__(self):
        self.logger = get_logger()
        self.engine = engine
        self.SessionLocal = SessionLocal
        self.session = self.SessionLocal()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None, data: Optional[Dict] = None, method: str = 'GET') -> str:
        """
        Fetches a web page using the specified HTTP method.
        Args:
            url (str): The URL of the web page to fetch.
            headers (Optional[Dict], optional): HTTP headers to include in the request. Defaults to None.
            params (Optional[Dict], optional): Query parameters to include in the request. Defaults to None.
            data (Optional[Dict], optional): Data to include in the request body (for POST requests). Defaults to None.
            method (str, optional): The HTTP method to use ('GET' or 'POST'). Defaults to 'GET'.
        Returns:
            str: The content of the response.
        Raises:
            HTTPError: If an HTTP error occurs during the request.
            Timeout: If the request times out.
            RequestException: If a general request exception occurs.
            Exception: If an unexpected error occurs.
        Logs:
            Logs errors with details about the exception and the URL.
        """
        try:
            swift = ProxyInterface(cachePeriod=1, maxProxies=10, autoRotate=True, protocol="https",)
            swift.update()
            if method.upper() == 'POST':
                response = requests.post(url, headers=headers or {}, params=params, data=data)
                full_url = response.url
            else:
                response = requests.get(url, headers=headers or {}, params=params, proxies=swift.get().as_requests_dict())
                full_url = response.url

            self.logger.info(f"Full URL: {full_url}")

            response.raise_for_status()
            return response.content

        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err} - URL: {url}")
            raise
        except Timeout as timeout_err:
            self.logger.error(f"Timeout error occurred: {timeout_err} - URL: {url}")
            raise
        except RequestException as req_err:
            self.logger.error(f"Request exception occurred: {req_err} - URL: {url}")
            raise
        except Exception as err:
            self.logger.error(f"An unexpected error occurred: {err} - URL: {url}")
            raise

    def get_extraction_dates(self, source_name: str, config: dict) -> tuple[datetime, datetime]:
        """
        Retrieves the extraction date range for a given data source.
        The method determines the start and end dates for data extraction based on the following priority:
        1. If the source has a corresponding entry in the `WebConfig` table with a `start_date`, 
           use the `start_date` and `end_date` from `WebConfig`. If `end_date` is not specified, 
           use the current date as the `end_date`.
        2. If no entry is found in `WebConfig`, check the `PriceStandardized` table for the latest 
           `source_date` associated with the source. Use this date as the `start_date` and the 
           current date as the `end_date`.
        3. If neither `WebConfig` nor `PriceStandardized` provides a date, fall back to the 
           `start_date` specified in the provided `config` dictionary. Use the current date as 
           the `end_date`.
        Args:
            source_name (str): The name of the data source for which to retrieve the extraction dates.
            config (dict): A configuration dictionary that may contain a fallback `start_date`.
        Returns:
            tuple[datetime, datetime]: A tuple containing the start date and end date for data extraction.
        Raises:
            ValueError: If the specified `source_name` is not found in the database.
        """

        source = self.session.query(Source).filter(Source.name == source_name).first()
        if not source:
            raise ValueError(f"Source '{source_name}' not found in database.")
        
        source_id = source.id

        # First: Check in WebConfig
        web_config = self.session.query(WebConfig).filter(WebConfig.source_id == source_id).first()
        if web_config and web_config.start_date:
            start_date = web_config.start_date
            end_date = web_config.end_date or datetime.today()
            return start_date, end_date

        # Second: If WebConfig not found, check in PriceStandardized for latest date
        latest_source_date = self.session.query(func.max(PriceStandardized.source_date)).filter(
            PriceStandardized.source_id == source_id
        ).scalar()

        if latest_source_date:
            start_date = latest_source_date
            end_date = datetime.today()
            return start_date, end_date

        # Third: Fallback to config file
        start_date = pd.to_datetime(config.get('start_date'))
        end_date = datetime.today()
        return start_date, end_date

    def extract_tables(self, html_content: str, required_headers: list, consider_empty_rows: bool) -> pd.DataFrame:
        """Extract tables from HTML content using TableExtractor"""
        extractor = TableExtractor(html_content, required_headers, consider_empty_rows)
        return extractor.to_dataframe()
    
    def is_model_empty(self, Model:any) -> bool:
        try:
            # Check if at least one record exists
            exists = self.session.query(Model.id).first() is not None
            self.logger.info(f"Model {Model.__name__} is empty: {not exists}")
            return exists
        except Exception as e:
            self.logger.error(f"Error checking Currency data: {e}")
            return False