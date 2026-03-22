-- checking the unique genders in the gold dimension products
SELECT DISTINCT Gender
FROM dw_gold.dim_customers;

-- Checking the gold dimension product has any duplicates
SELECT
  product_number,
  count(*) product_count
FROM dw_gold.dim_products
GROUP BY product_number
HAVING count(*) > 1;

SELECT * FROM dw_gold.dim_customers;
SELECT * FROM dw_gold.dim_products;
SELECT * FROM dw_gold.facts_sales;

-- Foreign key Integrity (Dimensions)
SELECT *
FROM dw_gold.facts_sales g
LEFT JOIN dw_gold.dim_customers c
ON c.customer_key = g.customer_key
LEFT JOIN dw_gold.dim_products p
ON p.product_key = g.product_key
WHERE p.product_key is NULL;
