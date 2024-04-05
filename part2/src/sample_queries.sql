-- Query to show where the index can have benefit.
SELECT COUNT(*)
FROM sales_link
WHERE source = 'Online'
  AND transaction_date BETWEEN '2024-01-01' AND '2024-12-31';

-- List of applied indexes in a table.
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'sales_link' AND indexdef ILIKE '%source%';


-- Retrieve all sales transactions with customer and product details:
SELECT sl.transaction_hash_key,
       sl.transaction_date,
       sl.transaction_amount,
       cs.customer_name,
       cs.customer_email,
       ps.product_name,
       ps.product_category
FROM sales_link sl
JOIN customers_satellite cs ON sl.customer_hash_key = cs.customer_hash_key
JOIN products_satellite ps ON sl.product_hash_key = ps.product_hash_key;


-- Count the number of sales transactions by source:
SELECT source, COUNT(*)
FROM sales_link
GROUP BY source;


-- Find the total sales amount for each product category:
SELECT ps.product_category, SUM(sl.transaction_amount) AS total_sales_amount
FROM sales_link sl
JOIN products_satellite ps ON sl.product_hash_key = ps.product_hash_key
GROUP BY ps.product_category;


-- List the top 10 customers with the highest total transaction amounts:
SELECT cs.customer_name, SUM(sl.transaction_amount) AS total_transaction_amount
FROM sales_link sl
JOIN customers_satellite cs ON sl.customer_hash_key = cs.customer_hash_key
GROUP BY cs.customer_name
ORDER BY total_transaction_amount DESC
LIMIT 10;


-- Identify the trend of total sales amount over time:
SELECT date_trunc('month', sl.transaction_date) AS transaction_month,
       SUM(sl.transaction_amount) AS total_sales_amount
FROM sales_link sl
GROUP BY transaction_month
ORDER BY transaction_month;

