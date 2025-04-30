from datetime import datetime
from typing import Any, Dict, List
import re
import time

import pandas as pd
from dateutil import parser
from bs4 import BeautifulSoup

from etl_pipeline.core.extract.base_extractor import BaseExtractor
from etl_pipeline.core.loader.base_loader import BaseLoader

from etl_pipeline.core.loader.transformed_data_store import StandardizedPriceWriter
from etl_pipeline.core.transform.base_transformer import BaseTransformer
from db.models.metadata import Currency
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

class ExchangeRateExtractor(BaseExtractor):
    def get_currency_code(self) -> list:
        """
        Retrieves all currency codes from the Currency table.

        Returns:
            List[str]: A list of currency codes (e.g., ['USD', 'EUR', 'INR']).
        """
        try:
            currency_codes = self.session.query(Currency.code).all()
            return [code[0] for code in currency_codes]  # Extract the first element from each tuple
        except Exception as e:
            self.logger.error(f"Failed to fetch currency codes: {str(e)}")
            return []
    def get_currency_options(self) -> dict:
        """
        Fetches available currency options from the FXTop historical exchange rates page.
        Its retrieve a dictionary of currency codes and their corresponding names.
        It looks for currency option in select boxes 'C1' on the page.
        Returns:
            dict: A dictionary where the keys are currency codes (e.g., 'USD', 'EUR') and
                  the values are the corresponding currency names (e.g., 'United States Dollar', 'Euro').
        """
        try:
            url = "https://fxtop.com/en/historical-exchange-rates.php"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            self.logger.info(f"Fetched HTML content from {url}")
            # Fetch the page content
            html_content = self.fetch_page(url=url, headers=headers, method="GET")
            soup = BeautifulSoup(html_content, 'html5lib')
            
            currency_dict = {}
            # Look in both select boxes C1
            for select_name in ['C1']:
                select = soup.find('select', {'name': select_name})
                self.logger.info(f"Fetched HTML C1 Options content from {select}")
                if not select:
                    continue

                for option in select.find_all('option'):
                    code = option.get('value')
                    name = option.text.strip()
                    if code and code not in currency_dict:
                        currency_dict[code] = name
            self.logger.info(f"Fetched currency details: {currency_dict}")
            return currency_dict
        except Exception as e:
            self.logger.error(f"Failed to fetch currency options: {str(e)}")
            return {}
    
    def fetch_exchange_rate_data(self, from_month, to_month, from_year, to_year, cur_from, cur_to) -> pd.DataFrame:
        """
        Fetches historical exchange rate data for a specified currency pair and date range.
        Args:
            from_month (int, optional): The starting month (1-12) for the data. Defaults to 1 (January).
            to_month (int, optional): The ending month (1-12) for the data. Defaults to 12 (December).
            from_year (int, optional): The starting year for the data. Defaults to 2018 (Base Year).
            to_year (int, optional): The ending year for the data. If None, defaults to the current year.
            cur_from (str, optional): The base currency code (e.g., "USD"). Defaults to "USD".
            cur_to (str, optional): The target currency code (e.g., "INR"). Defaults to "INR".
        Returns:
            pandas.DataFrame: A DataFrame containing the extracted exchange rate data with columns such as 
            ['Date', '%', 'Min', 'Average', 'Max', 'First', 'Last']
        Raises:
            Exception: If there is an issue with fetching the page content or extracting the tables.
        Notes:
            - This function constructs a query to the fxtop.com website to retrieve historical exchange rate data.
            - The function uses a helper method `fetch_page` to fetch the HTML content and `extract_tables` to parse the data.
            - Ensure that the helper methods `fetch_page` and `extract_tables` are implemented and handle errors appropriately.
        """
        try:
            # Set current year if to_year is not provided
            if to_year is None:
                to_year = datetime.now().year
            
            url = 'https://fxtop.com/en/historical-exchange-rates.php'
            params = {
                'C1': str(cur_from),
                'C2': str(cur_to),
                'DD1': '01',
                'DD2': '31',
                'MM1': str(from_month).zfill(2),    # Ensure two digits for month
                'MM2': str(to_month).zfill(2),      # Ensure two digits for month
                'YYYY1': str(from_year),
                'YYYY2': str(to_year),
                'A': '1',                           # Additional params as required by the site
                'B': '1',
                'I': '1',
                'P': '-2'
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-US,en;q=0.9',
            }

            # Fetch the page content
            html_content = self.fetch_page(url=url, headers=headers, params=params, method="GET")
            
            # Extract the relevant tables from the HTML content
            df = self.extract_tables(html_content, ['Date', '%', 'Min', 'Average', 'Max', 'First', 'Last'], consider_empty_rows=True)
            return df
        except Exception as e:
            self.logger.error(f"Failed to fetch exchange rate data: {str(e)}")
            return pd.DataFrame()

    def extract(self) -> Dict[str, Any]:
        try:
            try:
                # Check if Currency table is empty
                if not self.is_model_empty(Currency):
                    self.logger.info("Currency table is empty. Calling Currency Code Scrapper code for data.")
                    currency_dict = self.get_currency_options()
                    self.logger.info("Called Currency Code Scrapper code for data.")
                    if BaseLoader().insert_currencies_data(currency_dict=currency_dict):
                        self.logger.info("Currency data inserted successfully.")
                    else:
                        self.logger.error("Failed to insert currency data.")
                        return False
                else:
                    self.logger.info("Currency table is not empty. Proceeding with data extraction.")

                currency_codes = self.get_currency_code()
                if currency_codes:
                    self.logger.info(f"Currency Codes: {currency_codes}")
                else:
                    self.logger.error("No currency codes found.")
                    return False
                
                currency_codes = ['USD','EUR','GBP','AUD','CAD','JPY','CHF','CNY','SEK','NZD','MXN','SGD','HKD','NOK','KRW','TRY','BRL','ZAR','INR']
                for year in range(2019, datetime.now().year + 1):
                    for month in range(1, 12, 4):
                        for code in currency_codes:
                            if code == "USD":
                                continue
                            self.logger.info(f"Fetching exchange rate data for {code} from USD")
                            df = self.fetch_exchange_rate_data(from_month=month, to_month=month+3, from_year=year, to_year=year, cur_from="USD", cur_to=code)
                            
                            if not df.empty:
                                self.logger.info(f"Data fetched successfully for {code} from USD")
                                time.sleep(5)
                                self.logger.info(f"Data fetched From {month}-{year} to {month+3}-{year}: \n{df.head(5)}")
                                self.logger.info(f"Data fetched From {month}-{year} to {month+3}-{year}: {df.iloc[1].tolist()}")
                                BaseLoader().store_exchange_rate_dataframe(df=df, from_code="USD", to_code=code)
                                # Save the data to the database or perform further processing
                                # For example: save_to_database(df)
                            else:
                                self.logger.error(f"No data found for {code} from USD")

            except Exception as e:
                self.logger.error(f"Failed to extract data for: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error during extraction: {str(e)}")
            return False
    
def main():
    extractor = ExchangeRateExtractor()
    extracted_data = extractor.extract()
    if extracted_data:
        print("Data extraction completed successfully.")
    else:
        print("Data extraction failed.")

    # transformer = SunsirsTransformer()
    # transformed_data = transformer.transform()
    
    # if transformed_data:
    #     self.logger.info("Data transformation completed successfully.")
    # else:
    #     self.logger.error("Data transformation failed.")