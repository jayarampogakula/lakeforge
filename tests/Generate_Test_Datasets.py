# Databricks notebook source
# MAGIC %md
# MAGIC # Test Dataset Generator
# MAGIC 
# MAGIC Generate realistic test CSV files for LakeForge validation:
# MAGIC - Clean baseline datasets
# MAGIC - Corrupted datasets (for chaos testing)
# MAGIC - Schema drift versions
# MAGIC - Join explosion data
# MAGIC - Null spike data

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Configuration
DATA_DIR = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/data"
dbutils.fs.mkdirs(DATA_DIR)

print(f"✅ Data directory created: {DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate CLEAN Customers (1000 rows)

# COMMAND ----------

# Clean customer data
np.random.seed(42)
random.seed(42)

customer_ids = range(1, 1001)
customers = pd.DataFrame({
    'customer_id': customer_ids,
    'customer_name': [f'Customer_{i:04d}' for i in customer_ids],
    'email': [f'customer{i}@example.com' for i in customer_ids],
    'phone': [f'+1-555-{random.randint(1000,9999)}' for _ in customer_ids],
    'registration_date': [
        (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
        for _ in customer_ids
    ],
    'customer_status': np.random.choice(['Active', 'Inactive'], 1000, p=[0.9, 0.1]),
    'country': np.random.choice(['US', 'UK', 'CA', 'AU'], 1000)
})

print(f"✅ Generated {len(customers)} clean customer records")
display(customers.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate CLEAN Transactions (5000 rows)

# COMMAND ----------

# Clean transaction data - all reference valid customers
transactions = pd.DataFrame({
    'transaction_id': range(1, 5001),
    'customer_id': np.random.choice(range(1, 1001), 5000),  # All valid customer IDs
    'transaction_date': [
        (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d')
        for _ in range(5000)
    ],
    'transaction_amount': np.round(np.random.uniform(10.00, 5000.00, 5000), 2),
    'product_category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Toys'], 5000),
    'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash'], 5000),
    'transaction_status': np.random.choice(['Completed', 'Pending', 'Cancelled'], 5000, p=[0.85, 0.10, 0.05])
})

print(f"✅ Generated {len(transactions)} clean transaction records")
display(transactions.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save Clean Datasets

# COMMAND ----------

# Save to CSV
customers.to_csv(f"/dbfs{DATA_DIR}/good_customers.csv", index=False)
transactions.to_csv(f"/dbfs{DATA_DIR}/good_transactions.csv", index=False)

print("✅ Saved clean datasets:")
print(f"   - good_customers.csv ({len(customers)} rows)")
print(f"   - good_transactions.csv ({len(transactions)} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate BAD Dataset #1: Join Explosion
# MAGIC 
# MAGIC Create duplicate customer_ids to cause massive join explosion

# COMMAND ----------

# Duplicate 20% of customers 5 times each
bad_customers = customers.copy()
duplicates = bad_customers.sample(n=200, random_state=42).copy()

for _ in range(4):
    bad_customers = pd.concat([bad_customers, duplicates], ignore_index=True)

bad_customers = bad_customers.sample(frac=1, random_state=42).reset_index(drop=True)

print(f"❌ Created bad_join_customers.csv with {len(bad_customers)} rows (includes {len(duplicates)*4} duplicates)")
print(f"   This will cause join explosion: 5000 transactions → {len(bad_customers)/1000 * 5000:.0f} rows!")

bad_customers.to_csv(f"/dbfs{DATA_DIR}/bad_join_customers.csv", index=False)

# Check duplicates
dup_count = bad_customers[bad_customers.duplicated(subset=['customer_id'], keep=False)]
print(f"   Duplicate customer_ids: {len(dup_count)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate BAD Dataset #2: Schema Drift
# MAGIC 
# MAGIC Change transaction_amount from DECIMAL to STRING (currency format)

# COMMAND ----------

drift_transactions = transactions.copy()
drift_transactions['transaction_amount'] = drift_transactions['transaction_amount'].apply(
    lambda x: f"${x:,.2f}"  # Convert 1234.56 to "$1,234.56"
)

print(f"❌ Created bad_schema_drift.csv with datatype change:")
print(f"   BEFORE: transaction_amount = DECIMAL(10,2)")
print(f"   AFTER:  transaction_amount = STRING (currency format)")
print(f"   Example values: {drift_transactions['transaction_amount'].head(3).tolist()}")

drift_transactions.to_csv(f"/dbfs{DATA_DIR}/bad_schema_drift.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate BAD Dataset #3: Null Spike
# MAGIC 
# MAGIC Create 80% nulls in email column (was ~0%)

# COMMAND ----------

null_customers = customers.copy()
null_indices = np.random.choice(null_customers.index, size=int(len(null_customers) * 0.8), replace=False)
null_customers.loc[null_indices, 'email'] = None

null_pct = null_customers['email'].isna().sum() / len(null_customers) * 100

print(f"❌ Created bad_null_spike.csv with {null_pct:.1f}% nulls in email column")
print(f"   BEFORE: ~0% nulls")
print(f"   AFTER:  {null_pct:.1f}% nulls (spike of {null_pct:.1f}%!)")

null_customers.to_csv(f"/dbfs{DATA_DIR}/bad_null_spike.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generate BAD Dataset #4: Duplicate Business Keys
# MAGIC 
# MAGIC Create non-unique customer_ids (primary key violation)

# COMMAND ----------

dup_customers = customers.copy()
dup_indices = np.random.choice(dup_customers.index, size=int(len(dup_customers) * 0.3), replace=False)
dup_customers.loc[dup_indices, 'customer_id'] = np.random.choice(range(1, 100), size=len(dup_indices))

unique_ids = dup_customers['customer_id'].nunique()
total_rows = len(dup_customers)

print(f"❌ Created bad_duplicate_keys.csv with business key violations:")
print(f"   Total rows: {total_rows}")
print(f"   Unique customer_ids: {unique_ids}")
print(f"   Duplicates: {total_rows - unique_ids} records")

dup_customers.to_csv(f"/dbfs{DATA_DIR}/bad_duplicate_keys.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Generate BAD Dataset #5: Row Count Anomaly
# MAGIC 
# MAGIC Only 100 rows instead of expected 5000 (99% data loss!)

# COMMAND ----------

anomaly_transactions = transactions.head(100).copy()

expected = 5000
actual = len(anomaly_transactions)
loss_pct = (1 - actual/expected) * 100

print(f"❌ Created bad_row_count.csv with massive data loss:")
print(f"   Expected: {expected} rows")
print(f"   Actual:   {actual} rows")
print(f"   Data loss: {loss_pct:.1f}%!")

anomaly_transactions.to_csv(f"/dbfs{DATA_DIR}/bad_row_count.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Generate BAD Dataset #6: Anti-Join (Orphaned Records)
# MAGIC 
# MAGIC Create transactions that reference non-existent customer_ids

# COMMAND ----------

orphan_transactions = transactions.copy()
orphan_indices = np.random.choice(orphan_transactions.index, size=500, replace=False)
orphan_transactions.loc[orphan_indices, 'customer_id'] = np.random.choice(range(10000, 20000), size=500)

valid_customers = set(range(1, 1001))
orphaned = orphan_transactions[~orphan_transactions['customer_id'].isin(valid_customers)]

print(f"❌ Created bad_orphaned_transactions.csv with referential integrity violations:")
print(f"   Total transactions: {len(orphan_transactions)}")
print(f"   Orphaned records: {len(orphaned)} (no matching customer_id)")
print(f"   These will fail anti-join validation!")

orphan_transactions.to_csv(f"/dbfs{DATA_DIR}/bad_orphaned_transactions.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verification - List All Generated Files

# COMMAND ----------

files = dbutils.fs.ls(DATA_DIR)

print("=" * 80)
print("✅ ALL TEST DATASETS GENERATED")
print("=" * 80)

for file in files:
    if file.name.endswith('.csv'):
        df = pd.read_csv(f"/dbfs{file.path}")
        status = "✅ CLEAN" if file.name.startswith("good_") else "❌ CORRUPTED"
        print(f"{status} {file.name:40s} {len(df):5d} rows")

print("=" * 80)
print("\n🚀 Ready for validation testing!")
print("\nNext step: Run BRONZE_Ingestion_Pipeline notebook")

