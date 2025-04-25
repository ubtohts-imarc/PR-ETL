import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from utility.logger import get_logger

logger = get_logger()

class TableExtractor:
    """
    !!!needs to add comments
    """
    def __init__(self, html_content: str, required_headers: List[str]):
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.required_headers = [header.lower() for header in required_headers]
        self.tables = self.soup.find_all('table')

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
                    if cells and any(cell.strip() for cell in cells):  
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