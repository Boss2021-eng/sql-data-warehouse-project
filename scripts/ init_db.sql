/* =========================================================
   DATA WAREHOUSE INITIAL SETUP
   ---------------------------------------------------------
   This script creates three chemas for a
   layered Data Warehouse architecture.

   Architecture Layers:
   1. Bronze  - Raw data ingestion from source systems
   2. Silver  - Cleaned and transformed datasets
   3. Gold    - Aggregated, analytics-ready datasets

   ---------------------------------------------------------
   WARNING
   ---------------------------------------------------------
   - This script may DROP the existing DataWarehouse database.
   - Running this script will permanently delete all data
     stored in the database if it already exists.
   - Ensure that any important data has been backed up
     before executing this script.
   - Use only in development or controlled environments.

   ========================================================= */


/* Use MySQL system database to ensure permission
   to create a new database */
USE mysql;

/* ---------------------------------------------------------
   Create Bronze Layer
   Stores raw data exactly as received from source systems
   --------------------------------------------------------- */
CREATE SCHEMA IF NOT EXISTS dw_Bronze;


/* ---------------------------------------------------------
   Create Silver Layer
   Stores cleaned and standardized data
   --------------------------------------------------------- */
CREATE SCHEMA IF NOT EXISTS dw_Silver;


/* ---------------------------------------------------------
   Create Gold Layer
   Stores aggregated and analytics-ready datasets
   --------------------------------------------------------- */
CREATE SCHEMA IF NOT EXISTS dw_Gold;


/* Optional: verify that schemas were created successfully */
SHOW DATABASES LIKE 'dw_%';
