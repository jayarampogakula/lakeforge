-- Silver Layer Transformation Query: sample_data_clean
-- Cleanse, cast datatypes, standardize strings, and derive indicators from Bronze
SELECT
  trim(record_id) AS record_id,
  trim(tenant_id) AS tenant_id,
  initcap(trim(customer_name)) AS customer_name,
  lower(trim(contact_email)) AS primary_email,
  cast(transaction_amount AS double) AS transaction_amount,
  upper(coalesce(region, 'GLOBAL')) AS region,
  cast(event_timestamp AS timestamp) AS transaction_timestamp,
  CASE WHEN transaction_amount >= 1000.0 THEN true ELSE false END AS is_high_value
FROM {source_table}
WHERE record_id IS NOT NULL AND transaction_amount > 0
