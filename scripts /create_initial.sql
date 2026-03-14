/* =========================================================
   DATA WAREHOUSE INITIAL SETUP
   ---------------------------------------------------------
   This script creates the base database and schemas for a
   layered Data Warehouse architecture.

   Architecture Layers:
   1. Bronze  - Raw data ingestion from source systems
   2. Silver  - Cleaned and transformed datasets
   3. Gold    - Aggregated, analytics-ready datasets
   ========================================================= */


/* Use MySQL system database to ensure permission
   to create a new database */
USE mysql;


/* Create the main Data Warehouse database
   IF NOT EXISTS prevents errors if it already exists */
CREATE DATABASE IF NOT EXISTS DataWarehouse;


/* Switch context to the Data Warehouse database */
USE DataWarehouse;


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
