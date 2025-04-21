from abc import ABC, abstractmethod
from typing import Any, Dict
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import pandas as pd

from transform.uom_conversion import UOMConverter
from utility.logger import get_logger

logger = get_logger()
class BaseTransformer(ABC):
    # def __init__(self, transformation_rules: Dict[str, Any]):
    #     self.transformation_rules = transformation_rules

    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data according to rules"""
        pass

    def transform_number(self, field: str, value: Any) -> Any:
        """Transform numeric fields"""
        rules = self.transformation_rules.get(field, {})
        if rules.get('type') != 'float':
            return value

        try:
            return round(float(value), rules.get('decimal_places', 2))
        except ValueError:
            logger.warning(f"Could not transform {field} to float: {value}")
            return value

    def transform_string(self, field: str, value: Any) -> str:
        """Transform string fields"""
        rules = self.transformation_rules.get(field, {})
        if rules.get('type') != 'string':
            return str(value)

        transformed_value = str(value)
        if rules.get('trim', False):
            transformed_value = transformed_value.strip()
        if rules.get('lowercase', False):
            transformed_value = transformed_value.lower()

        return transformed_value
    
    def rename_columns(self, df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
        """
        Rename DataFrame columns based on a mapping dictionary.

        Args:
            df (pd.DataFrame): Input DataFrame.
            rename_map (dict): Dictionary where keys are substrings to search for in column names,
                            and values are the corresponding replacement text.

        Returns:
            pd.DataFrame: DataFrame with renamed columns.
        """
        column_mapping = {}
        for col in df.columns:
            logger.debug(f"Original column name: {col}")
            col_lower = col.lower()
            for search_text, replace_text in rename_map.items():
                if search_text.lower() in col_lower:
                    column_mapping[col] = replace_text
                    logger.debug(f"Renaming column '{col}' to '{replace_text}'")
                    break  # Stop searching once a match is found

        # Rename columns based on the mapping
        df = df.rename(columns=column_mapping)
        return df
    
    '''
    If in future you want to fetch Input metadata from API, uncomment the below code and use it.
    '''
    # @retry(stop=stop_after_attempt(3), 
    #        wait=wait_exponential(multiplier=1, min=4, max=10))
    # def fetch_input_metadata(self, source: str) -> Dict[str, Dict[str, Any]]:
    #     """Fetch external metadata for enrichment via API"""
    #     url = f"http://localhost:8090?&source={source}"
    #     try:
    #         logger.info(f"Fetching metadata from {url}")
    #         # response = requests.get(url, timeout=10)
    #         # response.raise_for_status()
    #         # metadata = response.json()

    #         metadata = {
    #             "source":"sunsirs",
    #             "data" : {
    #                 "Benzene": {
    #                     "Currency": "RMB",
    #                     "volume": "wet ton",
    #                     "country": "China",
    #                 },
    #                 "Tin ingot": {
    #                     "Currency": "RMB",
    #                     "volume": "wet ton",
    #                     "country": "China",
    #                 },
    #                 "Copper": {
    #                     "Currency": "RMB",
    #                     "volume": "gallon",
    #                     "country": "China",
    #                 },
    #                 "Silver": {
    #                     "Currency": "RMB",
    #                     "volume": "gram",
    #                     "country": "China",
    #                 }
    #             }
    #         }
    #         logger.info(f"Input metadata fetched: {metadata}")
    #         return metadata.get('data', {})
        
    #     except HTTPError as http_err:
    #         self.logger.error(f"HTTP error occurred: {http_err} - URL: {url}")
    #         raise
    #     except Timeout as timeout_err:
    #         self.logger.error(f"Timeout error occurred: {timeout_err} - URL: {url}")
    #         raise
    #     except RequestException as req_err:
    #         self.logger.error(f"Request exception occurred: {req_err} - URL: {url}")
    #         raise
    #     except Exception as err:
    #         self.logger.error(f"An unexpected error occurred: {err} - URL: {url}")
    #         raise

    '''
    If in future you want to merge Input metadata to DataFrame, uncomment the below code and use it.
    '''
    # def merge_input_metadata_to_df(self, df: pd.DataFrame, key_column: str, metadata: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    #     try:
    #         """Merge metadata into DataFrame based on matching key values"""
    #         if metadata is None or not metadata:
    #             logger.warning("No UOM metadata found.")
    #             return pd.DataFrame()
            
    #         if key_column not in df.columns:
    #             logger.warning(f"Key column '{key_column}' not found in DataFrame.")
    #             return pd.DataFrame()

    #         # Converting Column values to lowercase to match the keys in metadata
    #         df[key_column] = df[key_column].astype(str).str.strip().str.lower()

    #         # Converting metadata keys to lowercase to match with the dataframe
    #         metadata_normalized = {k.lower(): v for k, v in metadata.items()}

    #         # Initialize empty columns
    #         # If you're sure that in api response all the commodities have similar key,
    #         # you can pick first value and store its keys no need to iterate over all the commodities values
    #         all_fields = {k for v in metadata_normalized.values() for k in v.keys()}
    #         # Adding all the new metadata keys to dataframe as column
    #         for field in all_fields:
    #             if field not in df.columns:
    #                 df[field] = None

    #         # Apply metadata values row-wise
    #         for idx, row in df.iterrows():
    #             key = row[key_column]
    #             if key in metadata_normalized:
    #                 for meta_key, meta_value in metadata_normalized[key].items():
    #                     df.at[idx, meta_key] = meta_value
    #             else:
    #                 logger.debug(f"No metadata found for: {key}")
            
    #         # Drop rows where all columns in all_fields are NaN
    #         df = df.dropna(subset=all_fields, how='all')
            
    #         return df
    
    #     except Exception as e:
    #         logger.error(f"Error merging metadata: {e}")
    #         return pd.DataFrame()
    
    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_uom_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Fetch external metadata for enrichment via API"""
        url = f"http://localhost:8090/uom"
        try:
            logger.info(f"Fetching metadata from {url}")
            # response = requests.get(url, timeout=10)
            # response.raise_for_status()
            # metadata = response.json()

            metadata = {
                "conversion_rates": {
                    "area": {
                        "m2": 1,
                        "acre": 0.0002471053815,
                    },
                    "days": {
                        "mo": 1,
                        "yr": 12,
                    },
                    "energy": {
                        "kWh": 1,
                        "MWh": 0.001,
                        "MMBtu": 3412.14,
                    },
                    "mass": {
                        "kg": 1,
                        "tonne": 0.001,
                        "g": 1000,
                        "oz t": 0.0311035,
                        "ton (US)": 0.0011023113109244,
                        "lb": 2.20462,
                        "candy": 0.003937007,
                    },
                    "unit": {
                        "dozen": 1,
                        "gj": 1,
                        "1000 cans": 1,
                        "%": 1,
                        "1000 board feet": 1,
                    },
                    "volume": {
                        "L": 1,
                        "m3": 0.001,
                        "% vol/hl": 0.01,
                        "kL": 0.001,
                        "gal": 3.78541,
                        "bbl": 0.0062898108,
                    },
                },
                "convertible_types": {
                    "mass": "volume",
                },
            }

            logger.info(f"UOM metadata fetched: {metadata.get('conversion_rates', {})}")
            return metadata
        
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
    
    def merge_uom_metadata_to_df(self, df: pd.DataFrame, metadata: Any) -> pd.DataFrame:
        try:
            """Merge metadata into DataFrame based on matching key values"""
            
            if metadata is None or not metadata:
                logger.warning("No UOM metadata found.")
                return df
            
            df = UOMConverter(metadata).convert(df)
            return df
        
        except Exception as e:
            logger.error(f"Error merging metadata: {e}")
            return pd.DataFrame()