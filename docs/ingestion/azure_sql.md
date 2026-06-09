# Azure SQL Database Ingestion Guide

Load data from Azure SQL Database.

## Configuration

```python
from lakeforge.ingestion import AzureSQLLoader

spark = SparkSession.builder.appName("AzureSQLIngestion").getOrCreate()

loader = AzureSQLLoader(
    spark=spark,
    server="myserver.database.windows.net",
    database="production",
    user="etl_user",
    password=dbutils.secrets.get(scope="azure", key="sql-password")
)

df = loader.load_table("dbo.orders")

df.write.format("delta").mode("overwrite").save("/mnt/bronze/azure_orders")
```

## Best Practices

* Use managed identities when running in Azure
* Enable SSL/TLS encryption
* Monitor DTU/vCore usage
* Implement connection pooling
