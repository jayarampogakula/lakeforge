"""
Google Sheets Loader for LakeForge
Load data from Google Sheets spreadsheets
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

class GoogleSheetsLoader:
    """Load data from Google Sheets"""
    
    def __init__(
        self,
        spark: SparkSession,
        credentials_path: str
    ):
        """
        Initialize Google Sheets loader
        
        Args:
            spark: SparkSession instance
            credentials_path: Path to service account JSON key
        """
        self.spark = spark
        self.credentials_path = credentials_path
        self.logger = logging.getLogger(__name__)
    
    def load_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: str = "Sheet1",
        header: bool = True
    ) -> DataFrame:
        """
        Load data from Google Sheet
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Sheet/tab name
            header: Whether first row is header
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading Google Sheet: {spreadsheet_id}/{sheet_name}")
        
        # Using Google Sheets API via pandas
        try:
            import pandas as pd
            from google.oauth2 import service_account
            import gspread
            
            # Authenticate
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            gc = gspread.authorize(credentials)
            
            # Open spreadsheet and worksheet
            spreadsheet = gc.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # Get all values
            data = worksheet.get_all_values()
            
            # Convert to pandas DataFrame
            if header and data:
                pdf = pd.DataFrame(data[1:], columns=data[0])
            else:
                pdf = pd.DataFrame(data)
            
            # Convert to Spark DataFrame
            return self.spark.createDataFrame(pdf)
            
        except ImportError:
            self.logger.error("Required packages not installed: gspread, google-auth")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load Google Sheet: {e}")
            raise
    
    def load_range(
        self,
        spreadsheet_id: str,
        range_notation: str
    ) -> DataFrame:
        """
        Load specific range from Google Sheet
        
        Args:
            spreadsheet_id: Spreadsheet ID
            range_notation: A1 notation (e.g., "Sheet1!A1:D100")
        
        Returns:
            DataFrame
        """
        self.logger.info(f"Loading range: {range_notation}")
        
        try:
            import pandas as pd
            from google.oauth2 import service_account
            import gspread
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
            
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(spreadsheet_id)
            
            # Get range values
            data = spreadsheet.values_get(range_notation)
            values = data.get('values', [])
            
            if values:
                pdf = pd.DataFrame(values[1:], columns=values[0])
                return self.spark.createDataFrame(pdf)
            
            return self.spark.createDataFrame([], "col1 STRING")
            
        except Exception as e:
            self.logger.error(f"Failed to load range: {e}")
            raise
