# Jira Ingestion Guide

Load issues, projects, and metadata from Atlassian Jira into your Data Lakehouse.

## Overview

* Load Jira issues using JQL queries
* Support for Jira Cloud and Server
* Custom field extraction
* Incremental updates

## Prerequisites

* Jira instance URL
* API token (Cloud) or Personal Access Token (Server)
* Email address (for Cloud authentication)

## Configuration

### Jira Cloud

```python
from lakeforge.ingestion import JiraLoader

spark = SparkSession.builder.appName("JiraIngestion").getOrCreate()

loader = JiraLoader(
    spark=spark,
    jira_url="https://yourcompany.atlassian.net",
    auth_token=dbutils.secrets.get(scope="prod", key="jira-api-token"),
    email="your-email@company.com"
)

# Load issues
df = loader.load_issues(
    jql="project = PROJ AND status = 'In Progress' ORDER BY created DESC",
    max_results=1000
)

# Write to Bronze
df.write.format("delta").mode("overwrite").save("/mnt/bronze/jira_issues")
```

### Load with Custom Fields

```python
fields = [
    "summary",
    "status",
    "assignee",
    "created",
    "updated",
    "customfield_10001"  # Epic Link
]

df = loader.load_issues(
    jql="updated >= -7d",
    fields=fields
)
```

### Load Projects

```python
projects_df = loader.load_projects()
projects_df.write.format("delta").mode("overwrite").save("/mnt/bronze/jira_projects")
```

## Incremental Pattern

```python
from datetime import datetime, timedelta

# Get last 7 days of updates
last_week = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
jql = f"updated >= '{last_week}'"

df = loader.load_issues(jql=jql)
```

## Best Practices

* Use JQL filters to reduce API calls
* Paginate large result sets
* Handle rate limiting with retries
* Store raw JSON for custom fields
