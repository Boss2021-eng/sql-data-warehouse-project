-- Data cleaning for the bronze datawarehouse and inserting into the table
INSERT INTO dw_silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr
, cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE
	WHEN UPPER(TRIM(cst_marital_status)) = "S" THEN "Single"
    WHEN UPPER(TRIM(cst_marital_status)) = "M" THEN "Married"
    ELSE 'n/a'
END AS cst_marital_status,
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = "M" THEN "Male"
    WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female"
    ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM dw_bronze.crm_cust_info;

-- check for the content of the table
SELECT *
FROM dw_silver.crm_cust_info;

-- insertion for the second table crm_prd_info

-- DROP TABLE IF EXISTS crm_prd_info
INSERT INTO dw_silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_line, prd_start_dt, prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, 
    prd_nm,
    COALESCE(NULLIF(prd_line, ''), 0) AS prd_line,
	COALESCE(
    -- 1. Get the next value after partioning by the product key
    LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) ,
    
    -- 2 handle date that are not moving, get the first value 
	FIRST_VALUE(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt),
        
	-- 3. Final fallback: the last value (requires full window frame)
        LAST_VALUE(prd_end_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_end_dt
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ) - INTERVAL 1 DAY AS prd_start_dt,
    prd_end_dt

FROM dw_bronze.crm_prd_info;

-- check the contents of the table
SELECT *
FROM dw_silver.crm_prd_info;


-- Data cleaning and insertion for the sales details table
INSERT INTO dw_silver.crm_sales_details
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
		WHEN sls_order_dt < 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt < 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
	END AS sls_ship_dt,
    CASE 
		WHEN sls_due_dt < 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
	END AS sls_due_dt,
    
    -- Clean Quantity
    CASE 
        WHEN sls_quantity IS NULL OR sls_quantity < 0 THEN NULL
        ELSE sls_quantity
    END AS sls_quantity,

    -- Clean Price
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN NULL
        ELSE sls_price
    END AS sls_price,

    -- Correct Sales (final step, depends on cleaned values)
    CASE 
    -- 1. Defensive Check: If inputs are missing, the result must be NULL
    WHEN sls_quantity IS NULL OR sls_price IS NULL THEN NULL
    
    -- 2. Validation Check: If recorded sales don't match (Qty * Price), fix it
    WHEN sls_sales != (sls_quantity * sls_price) THEN (sls_quantity * sls_price)
    
    -- 3. Success: Keep original sales if it matches the math
    ELSE sls_sales
END AS sls_sales

FROM dw_bronze.crm_sales_details;

-- check for the content of the table
SELECT *
FROM dw_silver.crm_sales_details;

-- 4th table erm_cust_az12
INSERT INTO dw_silver.erm_cust_az12 ( CID, BDATE, GEN)
SELECT *
FROM(
SELECT  
	CID,
    CASE 
		WHEN BDATE > CURRENT_DATE() THEN NULL
        ELSE BDATE
	END AS BDATE,
    CASE 
		-- Replacing the white and internal spaces
		WHEN UPPER(TRIM(REGEXP_REPLACE(GEN, '\\s+', ''))) IN ('M', 'MALE') THEN 'male'
		WHEN UPPER(TRIM(REGEXP_REPLACE(GEN, '\\s+', ''))) IN ('F', 'FEMALE') THEN 'female'
        -- Replace the empty strings as null and finally with 'n/a'
		ELSE COALESCE(NULLIF((REGEXP_REPLACE(GEN, '\\s+', '')), ''), 'n/a')  
	END AS GEN
FROM dw_bronze.erm_cust_az12
WHERE CID NOT LIKE 'NAS%'
UNION
SELECT 
	CID, 
     CASE 
		WHEN BDATE > CURRENT_DATE() THEN NULL
        ELSE BDATE
	END AS BDATE,
    CASE 
		WHEN UPPER(TRIM(REGEXP_REPLACE(GEN, '\\s+', ''))) IN ('M', 'MALE') THEN 'male'
		WHEN UPPER(TRIM(REGEXP_REPLACE(GEN, '\\s+', ''))) IN ('F', 'FEMALE') THEN 'female'
		ELSE COALESCE(NULLIF((REGEXP_REPLACE(GEN, '\\s+', '')), ''), 'n/a')  
	END AS GEN
FROM dw_bronze.erm_cust_az12 
WHERE CID LIKE 'NAS%') t;

-- check for the content of the table
SELECT *
FROM dw_silver.erm_cust_az12;
INSERT INTO dw_silver.erm_loc_a101 (CID, CNTRY)
SELECT 
	REPLACE(CID, '-', '') AS CID,
    CASE 
		WHEN TRIM(REGEXP_REPLACE(CNTRY, '\\s+', '')) = "DE" THEN "Denmark"
		WHEN TRIM(REGEXP_REPLACE(CNTRY, '\\s+', '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REGEXP_REPLACE(CNTRY, '\\s+', '')) = '' THEN "N/A"
        -- Remove hidden control characters (Carriage Returns, Line Feeds, Tabs) and trim edges
        ELSE TRIM(REGEXP_REPLACE(CNTRY, '[\\r\\n\\t]+', ''))  
    END AS CNTRY -- Normalize or handle blank codes
FROM dw_bronze.erm_loc_a101;

-- Check the content of the table
SELECT *
FROM dw_silver.erm_loc_a101;
