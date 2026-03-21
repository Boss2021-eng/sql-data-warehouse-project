/* =========================================================
   DATA QUALITY & VALIDATION SCRIPT - SILVER LAYER
   ========================================================= */

/* =========================================================
   1. CUSTOMER INFORMATION TABLE (crm_cust_info)
   ========================================================= */

-- 1.1 Check for duplicate customer IDs
SELECT 
    cst_id,
    COUNT(*) AS record_count
FROM dw_silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- 1.2 Check for leading/trailing spaces in first name
SELECT cst_firstname
FROM dw_silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 1.3 Check for data consistency in gender and marital status
SELECT DISTINCT cst_gndr
FROM dw_silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM dw_silver.crm_cust_info;


/* =========================================================
   2. PRODUCT INFORMATION TABLE (crm_prd_info)
   ========================================================= */

-- 2.1 Inspect table structure
DESC dw_silver.crm_prd_info;

-- 2.2 Check for duplicate or NULL product IDs
SELECT 
    prd_id,
    COUNT(*) AS record_count
FROM dw_silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2.3 Check for unwanted spaces in product name
SELECT *
FROM dw_silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 2.4 Check for missing product line values
SELECT prd_key
FROM dw_silver.crm_prd_info
WHERE prd_line IS NULL;

-- 2.5 Validate date sequence using window function
SELECT 
    prd_id,
    prd_key,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_end_dt) OVER (
        PARTITION BY prd_key 
        ORDER BY prd_end_dt
    ) AS next_end_date
FROM dw_bronze.crm_prd_info;


/* =========================================================
   3. SALES DETAILS TABLE (crm_sales_details)
   ========================================================= */

-- 3.1 Check for unwanted spaces in order number
SELECT *
FROM dw_silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- 3.2 Validate referential integrity (product key)
SELECT *
FROM dw_silver.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key 
    FROM dw_silver.crm_prd_info
);

-- 3.3 Validate referential integrity (customer ID)
SELECT *
FROM dw_bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id 
    FROM dw_silver.crm_cust_info
);

-- 3.4 Identify specific missing customer
SELECT cst_id
FROM dw_silver.crm_cust_info
WHERE cst_id = 11000;

-- 3.5 Validate order date format and range
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM dw_silver.crm_sales_details
WHERE sls_order_dt <= 0 
   OR LENGTH(sls_order_dt) != 8 
   OR sls_order_dt > 20500101 
   OR sls_order_dt < 19000101;

-- 3.6 Validate shipping date format and range
SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM dw_silver.crm_sales_details
WHERE sls_ship_dt <= 0 
   OR LENGTH(sls_ship_dt) != 8 
   OR sls_ship_dt > 20500101 
   OR sls_ship_dt < 19000101;

-- 3.7 Validate due date format and range
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM dw_silver.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LENGTH(sls_due_dt) != 8 
   OR sls_due_dt > 20500101 
   OR sls_due_dt < 19000101;

-- 3.8 Check chronological integrity of dates
SELECT *
FROM dw_silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_ship_dt > sls_due_dt;

-- 3.9 Validate business rule: sales = quantity * price
SELECT *
FROM dw_silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/* =========================================================
   4. CUSTOMER AUXILIARY TABLE (erm_cust_az12)
   ========================================================= */

-- 4.1 Normalize and validate customer IDs against master table
SELECT *
FROM (
    SELECT SUBSTRING(CID, 4, LENGTH(CID)) AS CID, BDATE, GEN
    FROM dw_silver.erm_cust_az12
    WHERE CID LIKE 'NAS%'
    
    UNION
    
    SELECT CID, BDATE, GEN
    FROM dw_silver.erm_cust_az12 
    WHERE CID NOT LIKE 'NAS%'
) t
WHERE t.CID NOT IN (
    SELECT DISTINCT cst_key
    FROM dw_silver.crm_cust_info
);

-- 4.2 Identify future birth dates
SELECT BDATE
FROM dw_silver.erm_cust_az12
WHERE BDATE > CURRENT_DATE();

-- 4.3 Check gender consistency
SELECT DISTINCT GEN
FROM dw_silver.erm_cust_az12;


/* =========================================================
   5. LOCATION TABLE (erm_loc_a101)
   ========================================================= */

-- 5.1 Validate customer ID linkage
SELECT DISTINCT CID
FROM dw_silver.erm_loc_a101
WHERE CID NOT IN (
    SELECT cst_key
    FROM dw_silver.crm_cust_info
);

-- 5.2 Check distinct country values
SELECT DISTINCT CNTRY
FROM dw_silver.erm_loc_a101;


/* =========================================================
   6. PRODUCT CATEGORY TABLE (erm_px_cat_giv2)
   ========================================================= */

-- 6.1 Check for unwanted spaces in ID
SELECT ID
FROM dw_bronze.erm_px_cat_giv2
WHERE ID != TRIM(ID);

-- 6.2 Validate category values
SELECT DISTINCT CAT
FROM dw_bronze.erm_px_cat_giv2;

-- 6.3 Check for unwanted spaces in category
SELECT CAT
FROM dw_bronze.erm_px_cat_giv2
WHERE CAT != TRIM(CAT);

-- 6.4 Validate subcategory values
SELECT DISTINCT SUBCAT
FROM dw_bronze.erm_px_cat_giv2;

-- 6.5 Check for unwanted spaces in subcategory
SELECT SUBCAT
FROM dw_bronze.erm_px_cat_giv2
WHERE SUBCAT != TRIM(SUBCAT);

-- 6.6 Validate maintenance values
SELECT DISTINCT MAINTENANCE
FROM dw_bronze.erm_px_cat_giv2;

-- 6.7 Check for unwanted spaces in maintenance column
SELECT MAINTENANCE
FROM dw_bronze.erm_px_cat_giv2
WHERE MAINTENANCE != TRIM(MAINTENANCE);
