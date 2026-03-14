# Data Warehouse Architecture Setup (MySQL)

## Overview

This project demonstrates the foundational setup of a **layered Data Warehouse architecture** using **MySQL**. The structure follows the modern **Bronze–Silver–Gold data architecture pattern**, which organizes data into progressive stages of refinement for analytics and reporting.

The goal is to create a structured environment where raw data is ingested, transformed, and ultimately optimized for business intelligence.

---

# Architecture Design

The data warehouse is organized into **three logical layers**:

| Layer  | Schema      | Description                                                                                           |
| ------ | ----------- | ----------------------------------------------------------------------------------------------------- |
| Bronze | `dw_bronze` | Raw data ingestion layer. Stores data exactly as received from source systems.                        |
| Silver | `dw_silver` | Data cleansing and transformation layer. Data is standardized and validated.                          |
| Gold   | `dw_gold`   | Business-ready layer. Contains aggregated and curated datasets optimized for reporting and analytics. |

```
DataWarehouse
│
├── dw_bronze   → Raw source data
│
├── dw_silver   → Cleaned and transformed data
│
└── dw_gold     → Aggregated analytics-ready data
```

---

# Technologies Used

* **MySQL** – Relational Database Management System
* **SQL** – Data Definition and Data Manipulation
* **GitHub** – Version control and documentation

---

# Database Setup

The following SQL commands create the data warehouse and its schemas.

## Step 1: Use MySQL System Database

This ensures the session has access to create new databases.

```sql
USE mysql;
```

---

## Step 2: Create the Data Warehouse Database

```sql
CREATE DATABASE DataWarehouse;
```

---

## Step 3: Select the Database

```sql
USE datawarehouse;
```

---

## Step 4: Create the Schemas

The schemas represent the three layers of the data pipeline.

```sql
CREATE SCHEMA dw_Bronze;
CREATE SCHEMA dw_Silver;
CREATE SCHEMA dw_Gold;
```

---

# Data Flow Process

The data pipeline follows a structured transformation process.

```
Source Systems
      │
      ▼
Bronze Layer
(Raw Ingestion)
      │
      ▼
Silver Layer
(Data Cleaning & Transformation)
      │
      ▼
Gold Layer
(Business Analytics & Reporting)
```

### Bronze Layer

* Stores raw extracted data
* No transformations applied
* Used for auditing and traceability

### Silver Layer

* Data cleaning and normalization
* Removal of duplicates
* Schema standardization

### Gold Layer

* Aggregated datasets
* Optimized for dashboards and reporting
* Used by BI tools and analysts

---

# Best Practices

1. **Keep Bronze immutable**
   Raw data should never be modified.

2. **Apply transformations only in Silver**

3. **Aggregate and optimize data in Gold**

4. **Use consistent naming conventions**

   * `dw_bronze`
   * `dw_silver`
   * `dw_gold`

5. **Maintain version control of SQL scripts in GitHub**

---

---

# Repository Structure

```
datawarehouse-project
│
├── sql
│   ├── create_database.sql
│   ├── create_schemas.sql
│   └── create_tables.sql
│
├── docs
│   └── architecture.md
│
└── README.md


# License

This project is released under the MIT License.
