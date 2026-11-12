-- Gold Layer Aggregation Query: customer_sales_mart
-- Join Silver orders and customers, calculate revenue metrics, and group by customer & date dimensions
SELECT
  c.customer_id,
  c.customer_name,
  c.region,
  YEAR(o.transaction_timestamp) AS order_year,
  MONTH(o.transaction_timestamp) AS order_month,
  ROUND(SUM(o.transaction_amount), 2) AS total_revenue,
  COUNT(o.record_id) AS total_orders,
  ROUND(AVG(o.transaction_amount), 2) AS avg_order_value,
  MAX(o.transaction_amount) AS max_order_amount,
  MIN(o.transaction_amount) AS min_order_amount,
  SUM(CASE WHEN o.is_high_value = true THEN 1 ELSE 0 END) AS high_value_order_count,
  MIN(o.transaction_timestamp) AS first_transaction_time,
  MAX(o.transaction_timestamp) AS last_transaction_time
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.transaction_amount > 0 AND c.customer_id IS NOT NULL
GROUP BY
  c.customer_id,
  c.customer_name,
  c.region,
  YEAR(o.transaction_timestamp),
  MONTH(o.transaction_timestamp)
HAVING COUNT(o.record_id) >= 1
ORDER BY total_revenue DESC, customer_id ASC
