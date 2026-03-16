/* =====================================================
   DATA WAREHOUSE – BRONZE LAYER DATA INGESTION SCRIPT
   Description:
   This script loads raw CSV data from CRM and ERP systems
   into the Bronze layer (dw_bronze schema).

   NOTE:
   Replace the empty string in LOAD DATA LOCAL INFILE ('')
   with the correct file path to the CSV file.
   ===================================================== */


/* =====================================================
   1. LOAD CRM CUSTOMER INFORMATION
   ===================================================== */

-- Optional: Clear table before loading new data
-- TRUNCATE TABLE dw_bronze.crm_cust_info;

LOAD DATA LOCAL INFILE ''
-- File path should point to cust_info.csv
INTO TABLE dw_bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Optional cleanup: remove first row if needed
DELETE FROM dw_bronze.crm_cust_info
ORDER BY cst_id ASC
LIMIT 1;


/* =====================================================
   2. LOAD CRM PRODUCT INFORMATION
   ===================================================== */

-- TRUNCATE TABLE dw_bronze.crm_prd_info;

LOAD DATA LOCAL INFILE ''
-- File path should point to prd_info.csv
INTO TABLE dw_bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   3. LOAD CRM SALES DETAILS
   ===================================================== */

-- TRUNCATE TABLE dw_bronze.crm_sales_details;

LOAD DATA LOCAL INFILE ''
-- File path should point to sales_details.csv
INTO TABLE dw_bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   4. LOAD ERP CUSTOMER DATA
   ===================================================== */

-- TRUNCATE TABLE dw_bronze.erm_cust_az12;

LOAD DATA LOCAL INFILE ''
-- File path should point to CUST_AZ12.csv
INTO TABLE dw_bronze.erm_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   5. LOAD ERP LOCATION DATA
   ===================================================== */

-- TRUNCATE TABLE dw_bronze.erm_loc_a101;

LOAD DATA LOCAL INFILE ''
-- File path should point to LOC_A101.csv
INTO TABLE dw_bronze.erm_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   6. LOAD ERP PRODUCT CATEGORY DATA
   ===================================================== */

TRUNCATE TABLE dw_bronze.erm_px_cat_giv2;

LOAD DATA LOCAL INFILE ''
-- File path should point to PX_CAT_G1V2.csv
INTO TABLE dw_bronze.erm_px_cat_giv2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   7. DATA VALIDATION QUERIES
   These queries confirm that data was successfully loaded
   ===================================================== */

-- View ERP Product Categories
SELECT *
FROM dw_bronze.erm_px_cat_giv2;

-- View CRM Customer Data
SELECT *
FROM dw_bronze.crm_cust_info;

-- View CRM Product Information
SELECT *
FROM dw_bronze.crm_prd_info;

-- View CRM Sales Details
SELECT *
FROM dw_bronze.crm_sales_details;

-- View ERP Customer Data
SELECT *
FROM dw_bronze.erm_cust_az12;
