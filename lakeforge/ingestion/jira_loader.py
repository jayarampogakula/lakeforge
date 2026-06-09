"""
Jira Data Loader for LakeForge
Supports loading issues, projects, and metadata from Atlassian Jira
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional, List
import requests
import logging
from datetime import datetime

class JiraLoader:
    """Load data from Jira Cloud or Server via REST API"""
    
    def __init__(self, spark: SparkSession, jira_url: str, auth_token: str, email: Optional[str] = None):
        """
        Initialize Jira loader
        
        Args:
            spark: SparkSession instance
            jira_url: Jira instance URL (e.g., https://yourcompany.atlassian.net)
            auth_token: API token or PAT
            email: Email for basic auth (required for Jira Cloud)
        """
        self.spark = spark
        self.jira_url = jira_url.rstrip('/')
        self.auth_token = auth_token
        self.email = email
        self.logger = logging.getLogger(__name__)
        
        # Setup authentication
        if email:
            self.auth = (email, auth_token)
        else:
            self.headers = {"Authorization": f"Bearer {auth_token}"}
    
    def load_issues(
        self,
        jql: str = "order by created DESC",
        fields: Optional[List[str]] = None,
        max_results: int = 1000,
        expand: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Load Jira issues using JQL query
        
        Args:
            jql: JQL query string
            fields: List of fields to retrieve (None = all)
            max_results: Maximum results per page
            expand: Additional data to expand (changelog, transitions, etc.)
        
        Returns:
            DataFrame with issue data
        """
        self.logger.info(f"Loading Jira issues with JQL: {jql}")
        
        issues = []
        start_at = 0
        
        while True:
            params = {
                "jql": jql,
                "startAt": start_at,
                "maxResults": max_results
            }
            
            if fields:
                params["fields"] = ",".join(fields)
            if expand:
                params["expand"] = ",".join(expand)
            
            response = self._make_request("/rest/api/3/search", params)
            
            if not response:
                break
            
            batch = response.get("issues", [])
            issues.extend(batch)
            
            total = response.get("total", 0)
            start_at += len(batch)
            
            self.logger.info(f"Fetched {start_at}/{total} issues")
            
            if start_at >= total:
                break
        
        # Convert to DataFrame
        if issues:
            return self.spark.createDataFrame(issues)
        else:
            return self.spark.createDataFrame([], "key STRING, fields MAP<STRING, STRING>")
    
    def load_projects(self) -> DataFrame:
        """Load all accessible Jira projects"""
        self.logger.info("Loading Jira projects")
        
        response = self._make_request("/rest/api/3/project")
        
        if response:
            return self.spark.createDataFrame(response)
        return self.spark.createDataFrame([], "id STRING, key STRING, name STRING")
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make authenticated request to Jira API"""
        url = f"{self.jira_url}{endpoint}"
        
        try:
            if hasattr(self, 'auth'):
                response = requests.get(url, auth=self.auth, params=params, timeout=30)
            else:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Jira API request failed: {e}")
            return None
