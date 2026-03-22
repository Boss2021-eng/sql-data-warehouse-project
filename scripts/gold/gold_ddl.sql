/* =========================================================
   DATA WAREHOUSE - GOLD LAYER (STAR SCHEMA)
   =========================================================
   This layer contains business-ready, analytics-friendly views.
   It follows a STAR SCHEMA design:

   - Dimension Tables:
        1. dim_customers
        2. dim_products

   - Fact Table:
        1. facts_sales

   These views are built from cleaned SILVER layer tables.
   ========================================================= */


/* =========================================================
   1. CUSTOMER DIMENSION (dim_customers)
   =========================================================
   Purpose:
   Provides a unified customer view combining:
   - Core customer attributes
   - Demographics (gender, birthdate)
   - Geographic information

   Key Notes:
   - Surrogate key generated using ROW_NUMBER()
   - Gender is derived from multiple sources
   ========================================================= */

CREATE OR REPLACE VIEW dw_gold.dim_customers AS

SELECT 
    -- Surrogate key for dimension table
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- Business key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,

    -- Customer attributes
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,

    -- Gender resolution logic:
    -- Prefer CRM value unless marked as 'n/a', fallback to auxiliary table
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE ca.GEN
    END AS gender,

    ci.cst_marital_status AS marital_status,

    -- Demographic enrichment
    ca.BDATE AS birth_date,

    -- Geographic enrichment
    lo.CNTRY AS country

FROM dw_silver.crm_cust_info ci

-- Join to auxiliary customer table for demographics
LEFT JOIN dw_silver.erm_cust_az12 ca 
    ON ci.cst_key = ca.CID

-- Join to location table for country information
LEFT JOIN dw_silver.erm_loc_a101 lo
    ON ci.cst_key = lo.CID;



/* =========================================================
   2. PRODUCT DIMENSION (dim_products)
   =========================================================
   Purpose:
   Provides the latest snapshot of product attributes.

   Key Notes:
   - Handles Slowly Changing Dimensions (SCD Type 2-like)
   - Keeps only the most recent product record
   - Enriches product with category hierarchy
   ========================================================= */

CREATE OR REPLACE VIEW dw_gold.dim_products AS

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY start_date, product_number) AS product_key,

    product_id,
    product_number,
    product_name,

    -- Product hierarchy
    category_id,
    category,
    subcategory,

    -- Maintenance classification
    MAINTENANCE,

    -- Product attributes
    product_line,
    start_date

FROM (
    SELECT 
        -- Identify latest record per product
        ROW_NUMBER() OVER (
            PARTITION BY pd.prd_key 
            ORDER BY pd.prd_start_dt DESC, pd.prd_key
        ) AS order_num,

        pd.prd_id AS product_id,
        pd.prd_key AS product_number,
        pd.prd_nm AS product_name,

        -- Category mapping
        pd.cat_id AS category_id,
        px.CAT AS category,
        px.SUBCAT AS subcategory,
        px.MAINTENANCE,

        pd.prd_line AS product_line,
        pd.prd_start_dt AS start_date

    FROM dw_silver.crm_prd_info pd

    -- Join to product category lookup
    LEFT JOIN dw_silver.erm_px_cat_giv2 px
        ON pd.cat_id = px.ID
) t

-- Keep only the latest product record
WHERE t.order_num = 1;



/* =========================================================
   3. SALES FACT TABLE (facts_sales)
   =========================================================
   Purpose:
   Stores transactional sales data linked to dimensions.

   Grain:
   One row per sales transaction (order line level)

   Key Notes:
   - Links to both customer and product dimensions
   - Contains measurable metrics for analysis
   ========================================================= */

CREATE OR REPLACE VIEW dw_gold.facts_sales AS 

SELECT 
    -- Transaction identifier
    s.sls_ord_num AS order_number,

    -- Foreign keys
    c.customer_key,
    p.product_key,

    -- Dates
    s.sls_order_dt AS order_date,
    s.sls_ship_dt AS ship_date,
    s.sls_due_dt AS due_date,

    -- Measures
    s.sls_quantity AS quantity,
    s.sls_price AS price,
    s.sls_sales AS sales_amount

FROM dw_silver.crm_sales_details s

-- Join to product dimension
LEFT JOIN dw_gold.dim_products p
    ON s.sls_prd_key = p.product_number

-- Join to customer dimension
LEFT JOIN dw_gold.dim_customers c
    ON s.sls_cust_id = c.customer_id;
