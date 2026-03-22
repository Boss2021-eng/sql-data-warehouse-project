-- DATABASE EXPLORATION
-- Explore all objects in the database
SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('dw_bronze','dw_gold', 'dw_silver');

-- Checking every columns in a table
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA IN ('dw_bronze','dw_gold', 'dw_silver');


-- DATABASE EXPLORATION (distinct and unique values)
-- identify all the countries all our customers come from
SELECT DISTINCT country
FROM dw_gold.dim_customers;

-- identify all the categories for the products
SELECT DISTINCT category, subcategory, product_name FROM dw_gold.dim_products
ORDER BY 1,2,3;

-- DATE EXPLORATION 
-- Find the first and last date of order
SELECT 
	MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) AS order_range_months,
    TIMESTAMPDIFF(YEAR, MIN(order_date), MAX(order_date)) AS order_range_years
FROM dw_gold.facts_sales; 

-- Find the youngest and oldest customer
SELECT
	MIN(Birth_date) AS youngest_birth_date,
    timestampdiff(year, MIN(Birth_date),current_date) AS youngest_age,
    MAX(Birth_date) AS oldest_birth_date,
    timestampdiff(year, MAX(Birth_date),current_date) AS oldest_age
FROM dw_gold.dim_customers;
