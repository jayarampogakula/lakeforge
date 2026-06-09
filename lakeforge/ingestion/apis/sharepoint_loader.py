"""
SharePoint Data Loader for LakeForge
Supports loading documents and lists from SharePoint Online
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional, List
import requests
import logging

class SharePointLoader:
    """Load data from SharePoint Online via Microsoft Graph API"""
    
    def __init__(self, spark: SparkSession, tenant_id: str, client_id: str, client_secret: str):
        """
        Initialize SharePoint loader
        
        Args:
            spark: SparkSession instance
            tenant_id: Azure AD tenant ID
            client_id: Application (client) ID
            client_secret: Client secret
        """
        self.spark = spark
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = logging.getLogger(__name__)
        self.access_token = None
    
    def authenticate(self):
        """Get OAuth access token"""
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }
        
        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            self.access_token = response.json()["access_token"]
            self.logger.info("SharePoint authentication successful")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"SharePoint authentication failed: {e}")
            raise
    
    def load_site_documents(
        self,
        site_id: str,
        drive_id: Optional[str] = None,
        folder_path: str = "/"
    ) -> DataFrame:
        """
        Load documents from SharePoint site
        
        Args:
            site_id: SharePoint site ID
            drive_id: Document library drive ID (None = default)
            folder_path: Folder path within the drive
        
        Returns:
            DataFrame with document metadata
        """
        if not self.access_token:
            self.authenticate()
        
        self.logger.info(f"Loading documents from site: {site_id}")
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Get drive ID if not provided
        if not drive_id:
            drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
            drive_response = requests.get(drive_url, headers=headers, timeout=30)
            drive_response.raise_for_status()
            drive_id = drive_response.json()["id"]
        
        # Get items in folder
        items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder_path}:/children"
        
        items = []
        while items_url:
            response = requests.get(items_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            items.extend(data.get("value", []))
            items_url = data.get("@odata.nextLink")
        
        if items:
            return self.spark.createDataFrame(items)
        return self.spark.createDataFrame([], "id STRING, name STRING, webUrl STRING")
    
    def load_list_items(self, site_id: str, list_id: str) -> DataFrame:
        """
        Load items from SharePoint list
        
        Args:
            site_id: SharePoint site ID
            list_id: List ID
        
        Returns:
            DataFrame with list items
        """
        if not self.access_token:
            self.authenticate()
        
        self.logger.info(f"Loading list items from site: {site_id}, list: {list_id}")
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields"
        
        items = []
        while items_url:
            response = requests.get(items_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            items.extend(data.get("value", []))
            items_url = data.get("@odata.nextLink")
        
        if items:
            return self.spark.createDataFrame(items)
        return self.spark.createDataFrame([], "id STRING, fields MAP<STRING, STRING>")
