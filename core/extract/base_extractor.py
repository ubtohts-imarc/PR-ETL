from abc import ABC
from typing import Any, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import pandas as pd

from core.extract.table_extractor import TableExtractor
from utility.logger import get_logger

logger = get_logger()

class BaseExtractor(ABC):
    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_page(self, url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None, data: Optional[Dict] = None, method: str = 'GET') -> str:
        """Fetch page content with retry logic and error handling"""
        try:
            if method.upper() == 'POST':
                response = requests.post(url, headers=headers or {}, params=params, data=data)
            else:
                response = requests.get(url, headers=headers or {}, params=params)

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

    def extract_tables(self, html_content: str, required_headers: list) -> pd.DataFrame:
        """Extract tables from HTML content using TableExtractor"""
        extractor = TableExtractor(html_content, required_headers)
        return extractor.to_dataframe()