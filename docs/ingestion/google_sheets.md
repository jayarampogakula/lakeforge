# Google Sheets Ingestion Guide

Load data from Google Sheets spreadsheets.

## Prerequisites

* Service account with Sheets API access
* Spreadsheet ID
* Sheet shared with service account email

## Configuration

```python
from lakeforge.ingestion import GoogleSheetsLoader

spark = SparkSession.builder.appName("SheetsIngestion").getOrCreate()

loader = GoogleSheetsLoader(
    spark=spark,
    credentials_path="/dbfs/secrets/gcp-service-account.json"
)

# Load sheet
df = loader.load_sheet(
    spreadsheet_id="1abc...xyz",
    sheet_name="Sales Data",
    header=True
)

# Load specific range
df = loader.load_range(
    spreadsheet_id="1abc...xyz",
    range_notation="Sheet1!A1:D100"
)

df.write.format("delta").mode("overwrite").save("/mnt/bronze/sheets_data")
```

## Best Practices

* Use service accounts for automation
* Implement rate limiting
* Cache frequently accessed data
* Handle API quota limits
