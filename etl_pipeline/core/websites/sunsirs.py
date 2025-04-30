from datetime import datetime
from typing import Any, Dict, List
import re

import pandas as pd
from dateutil import parser

from etl_pipeline.core.extract.base_extractor import BaseExtractor
from etl_pipeline.core.loader.raw_data_reader import RawPriceFetcher
from etl_pipeline.core.loader.raw_data_store import RawPriceWriter
from etl_pipeline.core.loader.transformed_data_store import StandardizedPriceWriter
from etl_pipeline.core.transform.base_transformer import BaseTransformer
from utility.yaml_loader import load_yaml_config

config = load_yaml_config("etl_pipeline/core/config/websites/sunsirs.yaml")

class SunsirsTransformer(BaseTransformer):
    def transform(self) -> Dict[str, Any]:
        """
        Transforms raw data fetched from the "sunsirs" website by merging it with unit of measurement (UOM) metadata 
        and saving the standardized data.
        Returns:
            Dict[str, Any]: A dictionary indicating the success of the transformation process.
        
        Workflow:
        1. Fetch raw data using the `RawPriceFetcher` for the "sunsirs" source.
        2. Retrieve UOM metadata required for data transformation.
        3. Merge the UOM metadata into the fetched data to standardize the units.
        4. Save the transformed data using the `StandardizedPriceWriter` if the DataFrame is not empty.
        """
        try:
            fetcher = RawPriceFetcher("sunsirs")
            data = fetcher.fetch()
            self.logger.info(f"Fetched Data: {list(data.columns)}")

            website_uom_data = self.fetch_uom_metadata()
            if not website_uom_data:
                self.logger.error("No metadata fetched for transformation.")
                
            # Merge metadata into DataFrame
            if isinstance(data, dict):
                data = data.get('extracted_data', pd.DataFrame())
            
            uom_converted_data = self.merge_uom_metadata_to_df(df=data, metadata=website_uom_data)
            
            if not uom_converted_data.empty:
                writer = StandardizedPriceWriter(df=uom_converted_data, source_name="sunsirs")
                writer.save()
                self.logger.info(f"Data saved successfully for {len(uom_converted_data)} records.")
                return True
            return False
        
        except Exception as e:
            self.logger.error(f"Error during transformation: {str(e)}")
            return False

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
        self.logger.info(f"Headers: {headers}")

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
                self.logger.info(f"Successfully parsed date from header: {header} -> {parsed_date}")
            except Exception as e:
                self.logger.warning(f"Could not parse date from header value: {header}, Error: {str(e)}")
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
            self.logger.info(f"Filtering columns by date: {target_date}, Type: {type(target_date)}")

            filtered_df = pd.DataFrame()

            # Get required headers from config
            required_cols = config['required_headers']
            
            # Get date columns mapping
            date_columns = self.parse_header_dates(df.columns)
            
            # Find matching date columns
            matching_cols = []
            target_month_day = target_date.strftime("%m-%d")
            self.logger.info(f"Target Month-Day: {target_month_day}, Date Columns: {date_columns}")

            for col, parsed_date in date_columns.items():
                if parsed_date.strftime("%m-%d") == target_month_day:
                    matching_cols.append(col)
                    self.logger.info(f"Found matching date column: {col} for target date: {target_date}")
            
            self.logger.info(f"Matching columns for target date {target_date}, type: {type(target_date)}: {matching_cols}")
            
            if matching_cols:
                # Combine required columns and matching date columns of the same date
                # i.e requested date for sunsirs data is 2025-0328(%Y-%m%d).
                # It will return data for ['03-28', '03-31'] so we only consider 03-28 date's prices.
                columns_to_keep = required_cols + matching_cols
                filtered_df = df[columns_to_keep].copy()
 
                # Rename the matching date column to 'Price' if exists
                if matching_cols:
                    filtered_df = filtered_df.rename(columns={matching_cols[0]: 'Price'})
                    
                # Creating new column with date values
                filtered_df['Date'] = pd.to_datetime(target_date.strftime('%Y-%m-%d'))
                self.logger.info(f"After Target Date: {target_date} Filtered DataFrame columns: {filtered_df.columns}")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Error filtering columns by date: {str(e)}")
            return df
    
    def extract(self) -> Dict[str, Any]:
        """
        Extracts data from a specified source, processes it, and saves the results.
        This method performs the following steps:
        1. Iterates over a range of dates from the configured start date to today.
        2. Fetches HTML content from a dynamically generated URL for each date.
        3. Extracts tables from the HTML content based on required headers.
        4. Filters the extracted data by date and applies necessary transformations.
        5. Concatenates the filtered data into a final DataFrame.
        6. Renames columns in the final DataFrame to standardized names.
        7. Saves the processed data using a `RawPriceWriter`.
        Returns:
            bool: True if data was successfully extracted and saved, False otherwise.
        Raises:
            Exception: Logs and handles any exceptions that occur during the extraction process.
        Notes:
            - The method uses configurations such as `start_date`, `base_url`, and `required_headers` 
              from a `config` dictionary.
            - Logs detailed information about the extraction process, including warnings for missing data 
              and errors for failed operations.
            - The final DataFrame is saved only if it contains data.
        """
        try:
            start_date, end_date = self.get_extraction_dates(source_name="sunsirs", config=config)
            dates = pd.date_range(start_date, end_date)
            self.logger.info(f"Extraction Dates: {start_date} to {end_date}")
            final_df = pd.DataFrame()

            for date in dates:
                try:
                    url = config['base_url'].replace("REPLACEDATEHERE", date.strftime('%Y-%m%d'))
                    self.logger.info(f"Fetching data from {url}")
                    html_content = self.fetch_page(
                        url=url,
                        method="GET"
                    )

                    # Step-1: Extract the exact tables by providing required headers
                    df = self.extract_tables(html_content, config['required_headers'], consider_empty_rows=False)
                    
                    # Step-2: Convert date columns to datetime and filter by date
                    filtered_df = self.filter_columns_by_date(df, date)
                    
                    # Step-3: Apply unit mapping to the filtered DataFrame
                    if not filtered_df.empty:
                        self.logger.info(f"Extracted Date: {date}. ExtractedData: {filtered_df.head(2)}. ExtractedData Shape: {filtered_df.shape}")
                        final_df = pd.concat([final_df, filtered_df], ignore_index=True)
                    else:
                        self.logger.warning(f"No matching data found for date: {date}")

                except Exception as e:
                    self.logger.error(f"Failed to extract data for {date}: {str(e)}")
            
            self.logger.info(f"Final ExtractedData: {final_df.head(5)}\nExtractedData Shape: {final_df.shape}")

            if not final_df.empty:
                final_df = final_df.rename(columns={
                    'Commodity': 'product',
                    'Sectors': 'product_category',
                    'Price': 'price_value',
                    'Date': 'price_date'
                })
                writer = RawPriceWriter(df=final_df, source_name="sunsirs")
                writer.save()
                self.logger.info(f"Data saved successfully for {len(final_df)} records.")
                return True
            return False
        
        except Exception as e:
            self.logger.error(f"Error during extraction: {str(e)}")
            return False
    
def main():
    if config is None:
        self.logger.error("Failed to load configuration. Exiting.")
        return False
    
    extractor = SunsirsExtractor()
    extracted_data = extractor.extract()
    if extracted_data:
        self.logger.info("Data extraction completed successfully.")
    else:
        self.logger.error("Data extraction failed.")

    transformer = SunsirsTransformer()
    transformed_data = transformer.transform()
    
    if transformed_data:
        self.logger.info("Data transformation completed successfully.")
    else:
        self.logger.error("Data transformation failed.")