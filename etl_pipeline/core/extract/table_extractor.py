from typing import Any, Dict, List

import pandas as pd
from bs4 import BeautifulSoup

from utility.logger import get_logger
from typing import List, Dict, Any

logger = get_logger()

class TableExtractor:
    '''
    A utility class for extracting and processing HTML tables into structured data. 
    This class is designed to parse HTML content, extract tables, and convert them 
    into a unified pandas DataFrame while ensuring header consistency and filtering 
    out empty rows.
    Attributes:
        soup (BeautifulSoup): Parsed HTML content using BeautifulSoup.
        required_headers (List[str]): A list of required headers to identify relevant tables.
        tables (ResultSet): All table elements found in the HTML content.
    Example:
        ```
        # Sample HTML content
        html_content = """
        <html>
            <body>
                <table>
                    <tr><th>Name</th><th>Age</th></tr>
                    <tr><td>Alice</td><td>30</td></tr>
                    <tr><td>Bob</td><td>25</td></tr>
                </table>
                <table>
                    <tr><th>Product</th><th>Price</th></tr>
                    <tr><td>Book</td><td>10</td></tr>
                    <tr><td>Pen</td><td>2</td></tr>
                </table>
            </body>
        </html>
        
        # Required headers to filter tables
        required_headers = ["name", "age"]
        
        # Initialize TableExtractor
        extractor = TableExtractor(html_content, required_headers)
        
        # Extract tables and convert to DataFrame
        df = extractor.to_dataframe()
        print(df)
        ```
    Use Case:
        - Extracting structured data from HTML tables for further analysis.
        - Filtering tables based on specific headers to focus on relevant data.
        - Converting HTML table data into pandas DataFrame for easy manipulation and processing.
    Output for the example:
        ```
           Name  Age
        0  Alice   30
        1    Bob   25
        ```
    '''
    def __init__(self, html_content: str, required_headers: List[str], consider_empty_rows: bool = False):
        self.soup = BeautifulSoup(html_content, 'html5lib')
        self.required_headers = [header.lower() for header in required_headers]
        self.tables = self.soup.find_all('table')
        self.consider_empty_rows = consider_empty_rows

    def extract_text(self, element: Any) -> str:
        """Extract text from an HTML element, handling nested structures."""
        if isinstance(element, str):  
            return element.strip()
        return ' '.join(element.stripped_strings).strip()
    
    def extract_tables(self) -> List[Dict]:
        """Extract tables from HTML, ensuring header consistency and removing empty rows."""
        extracted_tables = []

        for table in self.tables:
            headers = None
            rows = []

            for row in table.find_all('tr'):
                cells = [self.extract_text(td) for td in row.find_all(['th', 'td'])]
                
                if not headers:  # First valid row as headers
                    if any(h.lower() in self.required_headers for h in cells):
                        headers = cells
                else:
                    # Remove rows that are empty or contain only whitespace
                    if cells and (True if self.consider_empty_rows else any(cell.strip() for cell in cells)):
                        rows.append(cells)

            if headers and rows:
                extracted_tables.append({'headers': headers, 'rows': rows})

        return extracted_tables
    
    def unify_tables(self, tables: List[Dict]) -> pd.DataFrame:
        """
        Merge all tables into a single DataFrame with consistent headers.
        !!!this function will be failed if the headers are not consistent
        """
        all_rows = []
        unified_headers = None
        
        for table in tables:
            headers = table['headers']
            if not unified_headers:
                unified_headers = headers
            
            df = pd.DataFrame(table['rows'], columns=headers)
            df = df.dropna(how='all').reset_index(drop=True)
            all_rows.append(df)

        return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=self.required_headers)
    
    def to_dataframe(self) -> pd.DataFrame:
        """Extract, filter, and return a unified DataFrame."""
        extracted_tables = self.extract_tables()
        return self.unify_tables(extracted_tables)