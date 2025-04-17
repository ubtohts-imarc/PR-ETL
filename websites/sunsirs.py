from typing import Dict, Any, List
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
import re

from extract.base_extractor import BaseExtractor
from extract.llm_extractor import extract_units
from loader.base_validator import BaseValidator
from transform.base_transformer import BaseTransformer
from utility.logger import get_logger
from utility.yaml_loader import load_yaml_config

logger = get_logger()
config = load_yaml_config("config/websites/sunsirs.yaml")

class SunsirsValidator(BaseValidator):
    def validate(self, data: Dict[str, Any]) -> bool:
        """Custom validation for Sunsirs data"""
        if not super().validate(data):
            return False
            
        required_fields = ['Product', 'Date', 'Initial Price']
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field: {field}")
                return False
                
        # Add more specific validation rules here
        return True

class SunsirsTransformer(BaseTransformer):
    def transform(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Transform extracted Sunsirs data"""
        website_input_data = self.fetch_input_metadata('sunsirs')
        if not website_input_data:
            logger.error("No metadata fetched for transformation.")
            
        # Merge metadata into DataFrame
        # logger.info(f"Initial Data: {type(data)}")
        if isinstance(data, dict):
            data = data.get('extracted_data', pd.DataFrame())
        
        data = self.merge_input_metadata_to_df(df=data, key_column='Commodity', metadata=website_input_data)
        logger.info(f"Input TransformedData: {data.head(100)}\nTransformedData Shape: {data.shape}")

        input_converted_data = self.rename_columns(rename_map={
            'Price': 'Input Price',
            'volume': 'Input Quantity Unit',
        }, df=data)
        logger.info(f"Rename TransformedData: {data.head(100)}\nTransformedData Shape: {data.shape}")

        website_uom_data = self.fetch_uom_metadata()
        if not website_uom_data:
            logger.error("No metadata fetched for transformation.")
            
        # Merge metadata into DataFrame
        # logger.info(f"Initial Data: {type(data)}")
        if isinstance(input_converted_data, dict):
            input_converted_data = input_converted_data.get('extracted_data', pd.DataFrame())
        
        uom_converted_data = self.merge_uom_metadata_to_df(df=input_converted_data, metadata=website_uom_data)
        logger.info(f"Final TransformedData: {uom_converted_data.head(100)}\nTransformedData Shape: {uom_converted_data.shape}")
        
        return uom_converted_data


class SunsirsExtractor(BaseExtractor):
    def parse_header_dates(self, headers: list) -> dict:
        """
        Parses a list of headers to extract and convert date-like strings into Python datetime objects.
        Args:
            headers (list): A list of header strings to be processed.
        Returns:
            dict: A dictionary where the keys are the original headers and the values are the parsed
                  datetime objects or None if the header could not be parsed as a date.
        Notes:
            - Headers that match any of the `required_headers` in the configuration are skipped.
            - Non-date headers or headers that cannot be parsed into a datetime object will have a value of None.
            - Special characters such as '%', '(', and ')' are removed from the headers before parsing.
            - Logs information about successfully parsed dates and errors encountered during parsing.
        """
        date_columns = {}
        logger.info(f"Headers: {headers}")

        for header in headers:
            try:
                # Skip non-date headers as we only want to convert date to the python datetime.
                if any(header.lower() in req_header.lower() for req_header in config['required_headers']):
                    continue
                    
                # Try to parse the date from header i.e '03-28', '03-31' to python datetime format.
                # Remove any percentage or special characters
                clean_header = re.sub(r'[%\(\)]', '', header.strip())
                parsed_date = parser.parse(clean_header)
                date_columns[header] = parsed_date
                logger.info(f"Successfully parsed date from header: {header} -> {parsed_date}")
            except Exception as e:
                logger.warning(f"Could not parse date from header value: {header}, Error: {str(e)}")
                # date_columns[header] = None
        return date_columns

    def filter_columns_by_date(self, df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """
        Filters the columns of a DataFrame based on a target date (date present in URL) and retains only the required columns 
        and the matching date column. The matching date column is renamed to 'Price', as it has information of price.
        and a new column 'Date' is added with the target date.
        
        Args:
            df (pd.DataFrame):  Scrapped dataframe.
                                Commodity,  Sectors,      03-28,      03-31,   Change
                                PA,	        Chemical, 237100.00,  244900.00,    3.29%
                                Urea,       Chemical,   1923.00,    1963.00,    2.08%
            target_date (datetime): The target date to filter columns by. datetime.datetime(2025-03-28 00:00:00)
        Returns:
            pd.DataFrame: A filtered DataFrame containing the required columns, the matching date column 
                          renamed to 'Price', and a new 'Date' column. If no matching columns are found 
                          or an error occurs, the original DataFrame is returned.
                            Commodity,   Sectors,     Price,        Date
                            PA,	        Chemical, 237100.00,  2025-03-28
                            Urea,       Chemical,   1923.00,  2025-03-28
        Raises:
            Exception: Logs an error message if any exception occurs during the filtering process.
        Notes:
            - The required columns are fetched from the yaml configuration (`config['required_headers']`).
            - The function matches columns based on the month and day of the target date.
            - If multiple matching date columns are found, only the first one is considered.
        """
        
        try:
            logger.info(f"Filtering columns by date: {target_date}, Type: {type(target_date)}")

            filtered_df = pd.DataFrame()

            # Get required headers from config
            required_cols = config['required_headers']
            
            # Get date columns mapping
            date_columns = self.parse_header_dates(df.columns)
            
            # Find matching date columns
            matching_cols = []
            target_month_day = target_date.strftime("%m-%d")
            
            logger.info(f"Target Month-Day: {target_month_day}")
            logger.info(f"Date Columns: {date_columns}")

            for col, parsed_date in date_columns.items():
                if parsed_date.strftime("%m-%d") == target_month_day:
                    matching_cols.append(col)
                    logger.info(f"Found matching date column: {col} for target date: {target_date}")
            
            logger.info(f"Matching columns for target date {target_date}, type: {type(target_date)}: {matching_cols}")
            if matching_cols:
                # Combine required columns and matching date columns of the same date
                # i.e requested date for sunsirs data is 2025-0328(%Y-%m%d).
                # It will return data for ['03-28', '03-31'] so we only consider 03-28 date's prices.
                columns_to_keep = required_cols + matching_cols

                # Filter DataFrame
                filtered_df = df[columns_to_keep].copy()
 
                # Rename the matching date column to 'Initial Price' if exists
                if matching_cols:
                    filtered_df = filtered_df.rename(columns={matching_cols[0]: 'Price'})
                
                logger.info(f"Before Target Date: {target_date} Filtered DataFrame columns: {filtered_df.columns}")
                # Creating new column with date values
                filtered_df['Date'] = pd.to_datetime(target_date.strftime('%Y-%m-%d'))

                # Reorder columns to have 'Date' at the end
                logger.info(f"After Target Date: {target_date} Filtered DataFrame columns: {filtered_df.columns}")
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"Error filtering columns by date: {str(e)}")
            return df
    
    # def extract_remarks_from_html(self, html: str) -> str:
    #     soup = BeautifulSoup(html, "html.parser")
    #     remarks_div = soup.find("div", string=lambda s: s and "Remarks" in s)
    #     if remarks_div:
    #         return remarks_div.get_text(strip=True).replace("Remarks:", "").strip()
    #     return ""
    
    def extract(self) -> Dict[str, Any]:
        """Extract data from Sunsirs website for date range"""
        start_date = pd.to_datetime('2025-04-15')
        end_date = pd.to_datetime('today')
        dates = pd.date_range(start_date, end_date)
        final_df = pd.DataFrame()

        for date in dates:
            try:
                url = config['base_url'].replace("REPLACEDATEHERE", date.strftime('%Y-%m%d'))
                logger.info(f"Fetching data from {url}")
                html_content = self.fetch_page(
                    url=url,
                    method="GET"
                )

                # # Step-1: Extract remarks from HTML content
                # remarks = self.extract_remarks_from_html(html_content)
                # logger.info(f"Remarks: {remarks}")
                # unit_mapping = extract_units(remarks=remarks, config=config)

                # Step-2: Extract the exact tables by providing required headers
                df = self.extract_tables(html_content, config['required_headers'])
                logger.info(f"Extracted DataFrame: {df.head(2)}. ExtractedData Shape: {df.shape}")
                
                # Step-3: Convert date columns to datetime and filter by date
                filtered_df = self.filter_columns_by_date(df, date)
                logger.info(f"Extracted Converted DataFrame: {filtered_df.head(2)}. ExtractedData Shape: {filtered_df.shape}")
                
                # Step-4: Apply unit mapping to the filtered DataFrame
                if not filtered_df.empty:
                    # def resolve_unit(row):
                    #     name = row['Commodity']
                    #     unit = unit_mapping.commodities.get(name, unit_mapping.default)
                    #     return pd.Series([unit.price, unit.weight])

                    # filtered_df[['price_unit', 'weight_unit']] = filtered_df.apply(resolve_unit, axis=1)

                    logger.info(f"Extracted Date: {date}. ExtractedData: {filtered_df.head(2)}. ExtractedData Shape: {filtered_df.shape}")
                    final_df = pd.concat([final_df, filtered_df], ignore_index=True)
                else:
                    logger.warning(f"No matching data found for date: {date}")

            except Exception as e:
                logger.error(f"Failed to extract data for {date}: {str(e)}")
                continue
        
        logger.info(f"Final ExtractedData: {final_df.head(100)}\nExtractedData Shape: {final_df.shape}")
        return {'extracted_data': final_df}
    
def main():
    if config is None:
        logger.error("Failed to load configuration. Exiting.")
        return False
    
    extractor = SunsirsExtractor()
    extracted_data = extractor.extract()
    if extracted_data:
        logger.info("Data extraction completed successfully.")
    else:
        logger.error("Data extraction failed.")

    transformer = SunsirsTransformer()
    transformed_data = transformer.transform(extracted_data)
    
    if not transformed_data.empty:
        logger.info("Data transformation completed successfully.")
    else:
        logger.error("Data transformation failed.")