/* =========================================================
   DATA WAREHOUSE Silver LAYER TABLES
   ---------------------------------------------------------
   This script creates all the initial silver layer tables
   for the Data Warehouse in MySQL. These tables store raw
   data as ingested from source systems. No transformations
   are applied at this stage.

   Layers:
   - silver: Raw ingestion tables

   DATA WARNING:
   - Running this script will DROP existing tables in the
     silver layer and remove all their data.
   - Only run in development or controlled environments.
========================================================= */

/* ---------------------------------------------------------
   CRM Customer Information Table
   - Stores customer master data in the silver layer
   - cst_id is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.crm_cust_info;

CREATE TABLE dw_silver.crm_cust_info (
    cst_id INT PRIMARY KEY,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);


/* ---------------------------------------------------------
   CRM Product Information Table
   - Stores product master data in the silver layer
   - prd_id is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.crm_prd_info;

CREATE TABLE dw_silver.crm_prd_info (
    prd_id INT PRIMARY KEY,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_line INT,
    prd_start_dt DATE,
    prd_end_dt DATE
);


/* ---------------------------------------------------------
   CRM Sales Details Table
   - Stores raw sales transactions
   - sls_ord_num is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.crm_sales_details;

CREATE TABLE dw_silver.crm_sales_details (
    sls_ord_num VARCHAR(50) PRIMARY KEY,
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_quantity INT,
    sls_price INT,
	sls_sales INT
);


/* ---------------------------------------------------------
   Customer Table - az12
   - Example customer table (ERM system)
   - CID is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.erm_cust_az12;

CREATE TABLE dw_silver.erm_cust_az12 (
	CID VARCHAR(50) PRIMARY KEY,
    BDATE DATE,
    GEN VARCHAR(50)
);


/* ---------------------------------------------------------
   Location Table - loca101
   - Stores customer location info
   - CID is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.erm_loc_a101;

CREATE TABLE dw_silver.erm_loc_a101 (
	CID VARCHAR(50) PRIMARY KEY,
    CNTRY VARCHAR(50)
);


/* ---------------------------------------------------------
   Product Category Table - px_CAT_GIV2
   - Stores product category hierarchy info
   - ID is the primary key
--------------------------------------------------------- */
DROP TABLE IF EXISTS dw_silver.erm_px_cat_GIV2;

CREATE TABLE dw_silver.erm_px_cat_GIV2(
	ID VARCHAR(50) PRIMARY KEY,
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
