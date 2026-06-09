# SharePoint Ingestion Guide

Load documents and lists from Microsoft SharePoint Online.

## Overview

* Load SharePoint documents and metadata
* Access SharePoint lists
* Microsoft Graph API integration
* OAuth authentication

## Prerequisites

* Azure AD app registration
* Tenant ID, Client ID, Client Secret
* SharePoint site permissions

## Configuration

### Setup Azure AD App

```python
from lakeforge.ingestion import SharePointLoader

spark = SparkSession.builder.appName("SharePointIngestion").getOrCreate()

loader = SharePointLoader(
    spark=spark,
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret=dbutils.secrets.get(scope="prod", key="sharepoint-secret")
)

# Authenticate
loader.authenticate()

# Load documents
docs_df = loader.load_site_documents(
    site_id="yoursite.sharepoint.com,guid,guid",
    folder_path="/Shared Documents/Reports"
)

# Write to Bronze
docs_df.write.format("delta").mode("overwrite").save("/mnt/bronze/sharepoint_docs")
```

### Load List Items

```python
list_df = loader.load_list_items(
    site_id="yoursite.sharepoint.com,guid,guid",
    list_id="list-guid-here"
)
```

## Azure AD App Registration

1. Register app in Azure Portal
2. Grant Microsoft Graph permissions:
   * Sites.Read.All
   * Files.Read.All
3. Create client secret
4. Grant admin consent

## Best Practices

* Use managed identities when possible
* Implement token refresh logic
* Handle large files appropriately
* Track document versions
