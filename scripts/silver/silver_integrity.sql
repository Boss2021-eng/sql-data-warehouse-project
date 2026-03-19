-- Cust_info table cleaning
-- check for null and duplicates
-- for the first table crm_cust_info
SELECT 
cst_id,
count(*) AS count_num
FROM dw_silver.crm_cust_info
GROUP BY cst_id
HAVING count(*)  > 1;

-- check for unnecessary spaces
SELECT cst_firstname
FROM dw_silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- check for data constitency and standardization
SELECT DISTINCT(cst_gndr)
FROM dw_silver.crm_cust_info;
SELECT DISTINCT(cst_marital_status)
FROM dw_silver.crm_cust_info


-- for the second table 
-- for the second table crm_prd_info
SELECT *
FROM dw_bronze.crm_prd_info;

DESC dw_bronze.crm_prd_info;

-- check for null
SELECT 
prd_id,
count(*)
FROM 
dw_silver.crm_prd_info
GROUP BY prd_id
having count(*) > 1 OR prd_id IS NULL;

-- check for unwanted spaces
SELECT *
FROM dw_bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Data Standardization and Constitency
SELECT  prd_key
FROM dw_bronze.crm_prd_info
WHERE prd_line IS NULL;

-- Correcting the date order
SELECT 
	prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt,
	LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt ) AS previous_end
FROM 
	dw_bronze.crm_prd_info
-- WHERE
-- prd_start_dt > prd_end_dt
Describe dw_bronze.crm_prd_info;

-- integrity check for sales details table
-- 1. check for nulls
SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM dw_bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- 2. check if all the unique id exists in the other connecting tables
SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM dw_bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM dw_silver.crm_prd_info);

-- check from dw_silver 
SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM dw_bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM dw_silver.crm_cust_info);

-- outstanding customer id 
SELECT cst_id FROM dw_silver.crm_cust_info
WHERE cst_id = 11000;

-- check for invalid dates for the order date 
SELECT  
	NULLIF(sls_order_dt,0) sls_order_dt
FROM dw_bronze.crm_sales_details
WHERE  sls_order_dt <= 0 OR LENGTH(sls_order_dt) !=8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

-- check for invalid dates for the ship date 
SELECT  
	NULLIF(sls_ship_dt,0) sls_ship_dt
FROM dw_bronze.crm_sales_details
WHERE  sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) !=8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

-- check for invalid dates for the due date 
SELECT  
	NULLIF(sls_due_dt,0) sls_due_dt
FROM dw_bronze.crm_sales_details
WHERE  sls_due_dt <= 0 OR LENGTH(sls_due_dt) !=8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

-- check for Invalid Date Orders (where the order date is less than the shipping due and less than the due date)
SELECT *
FROM dw_bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;

-- check for data constitency: Between sales, quantity and price
-- sales  = Quantity * Price
-- values must not be null
SELECT DISTINCT sls_sales, sls_quantity, sls_price 
FROM dw_bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price 

