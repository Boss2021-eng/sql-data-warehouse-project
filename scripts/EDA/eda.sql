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

-- Measures Exploration
-- 1. Find the Total Sales
SELECT SUM(sales_amount) FROM dw_gold.facts_sales AS total_sales_amount;

-- 2. Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM dw_gold.facts_sales;

-- 3. Find the average selling price
SELECT AVG(price) AS Average_selling_price FROM dw_gold.facts_sales ;

-- 4. Find the total number of orders
SELECT COUNT(order_number) AS Total_numbers_of_orders FROM dw_gold.facts_sales;
SELECT COUNT( DISTINCT order_number) AS Total_numbers_of_orders FROM dw_gold.facts_sales;

-- 5. Find the total numbers of products
SELECT COUNT(product_key) AS Total_numbers_of_products FROM dw_gold.dim_products;
SELECT COUNT(DISTINCT product_key) AS Total_numbers_of_products FROM dw_gold.dim_products;

-- 6. Find the total numbers of customers
SELECT COUNT(customer_key) AS Total_numbers_of_orders FROM dw_gold.dim_customers;
SELECT COUNT(DISTINCT customer_key) AS Total_numbers_of_customers FROM dw_gold.dim_customers;

-- 7. Find the total numbers of customers that has placed an order; 
SELECT 
	count(customer_id) total_customers
FROM dw_gold.dim_customers
WHERE EXISTS (
	SELECT 1
    FROM dw_gold.facts_sales
    WHERE dw_gold.dim_customers.customer_key = dw_gold.dim_customers.customer_key
);
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM dw_gold.facts_sales;

-- Generate a report reporting all key metrics
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM dw_gold.facts_sales 
UNION
SELECT 'Total Quantity',  SUM(quantity)  FROM dw_gold.facts_sales
UNION 
SELECT 'Average Selling Price', AVG(price) FROM dw_gold.facts_sales 
UNION 
SELECT 'Total Numbers of Orders' , COUNT( DISTINCT order_number)  FROM dw_gold.facts_sales
UNION 
SELECT 'Total Numbers of Products', COUNT(DISTINCT product_key)  FROM dw_gold.dim_products
UNION
SELECT 'Total Numbers of Customers', COUNT(DISTINCT customer_key)  FROM dw_gold.dim_customers;
