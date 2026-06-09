# MySQL Ingestion Guide

Load data from MySQL and MariaDB databases.

## Configuration

```python
from lakeforge.ingestion import MySQLLoader

spark = SparkSession.builder.appName("MySQLIngestion").getOrCreate()

loader = MySQLLoader(
    spark=spark,
    host="mysql.company.com",
    port=3306,
    database="ecommerce",
    user="etl_user",
    password=dbutils.secrets.get(scope="prod", key="mysql-password")
)

df = loader.load_table(
    table_name="orders",
    num_partitions=4,
    partition_column="order_id",
    lower_bound=1,
    upper_bound=500000
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/mysql_orders")
```

## Best Practices

* Use partitioned reads for large tables
* Create indexes on partition columns
* Enable binary logging for CDC
* Monitor replication lag
