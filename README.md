# sql-data-warehouse-project
Building a modern data warehouse with My SQL, including ETL processes, data modelling, and analytics

# 📊 Data Engineering & Analytics Project

## 📌 Project Overview
This project implements a robust **Data Engineering and Analytics pipeline** using the **Medallion Architecture (Bronze, Silver, Gold layers)** to transform raw data into actionable insights.  

The pipeline ensures data quality, scalability, and efficient querying, enabling advanced analytics and visualization for decision-making.

---

## 🎯 Objectives
- Design a scalable data pipeline for structured data processing  
- Improve data quality through systematic cleaning and transformation  
- Enable high-performance analytics using optimized data models  
- Deliver actionable insights through interactive dashboards  

---

## 🚀 Key Features
- End-to-end **ETL pipeline** (Extract, Transform, Load)  
- Layered architecture for data reliability and governance  
- Automated data cleaning and transformation workflows  
- Analytical data modeling using **Star Schema**  
- Interactive dashboards for business intelligence  

---

## 🛠️ Tools and Technologies
- **Programming:** Python (Pandas, NumPy)  
- **Database:** MySQL  
- **Version Control:** Git & GitHub  

---
## 📂 Data Sources

This project integrates data from two primary source systems:

- **CRM (Customer Relationship Management)** → `source_crm`  
- **ERP (Enterprise Resource Planning)** → `source_erp`  

All data is ingested into the **Bronze Layer (`dw_bronze`)** in its raw form using **flat files and bulk loading techniques**.

---



## 📥 Data Ingestion Method

### 🔹 Flat Files
- Source data is provided as **CSV/flat files** exported from CRM and ERP systems  
- Files retain original structure and formatting  

### 🔹 Bulk Insert
- Data is loaded into the Bronze schema using **bulk insert operations** for efficiency  
- Enables:
  - Fast ingestion of large datasets  
  - Minimal transformation during load  
  - Preservation of raw data integrity  
---

## 🏗️ Data Architecture (Medallion Architecture)

<p align="center">
  <img width="661" height="253" alt="image" src="https://github.com/user-attachments/assets/03e235da-841a-403c-8e8b-ce38b2bff02a" />
</p>

### 🥉 Bronze Layer (Raw Data)
- Stores **raw, unprocessed data** directly from source systems  
- Maintains original data format for traceability and auditing  
- No transformations applied  
- Acts as the **single source of truth**

---

### 🥈 Silver Layer (Cleaned & Processed Data)
- Data is **cleaned, validated, and standardized**  
- Handles:
  - Missing values  
  - Duplicate removal  
  - Data type corrections  
  - Data normalization  
- Ensures data consistency and reliability for downstream use  

---

### 🥇 Gold Layer (Business-Ready Data)
- Contains **aggregated and structured data** for analytics  
- Implements **Star Schema**:
  - Fact tables (measurable metrics)  
  - Dimension tables (descriptive attributes)  
- Optimized for **fast querying and reporting**

---

## 🧱 Data Modelling
- Designed using **Star Schema** for analytical efficiency  
- Fact tables capture key metrics (e.g., transactions, wait times)  
- Dimension tables provide context (e.g., date, category, location)  
- Ensures:
  - Reduced query complexity  
  - Improved performance  
  - Scalability  

---

## 🔄 Data Processing & Transformation
- Data ingested into Bronze layer from raw sources  
- Transformation steps in Silver layer:
  - Data cleansing  
  - Validation rules  
  - Standardization  
- Aggregations and feature engineering applied in Gold layer  


---

## 📈 Results
- Improved data quality and consistency  
- Reduced query time through optimized schema  
- Enhanced visibility into key metrics and trends  
- Enabled data-driven decision-making  

---

## 📎 Conclusion
This project demonstrates how a structured **Medallion Architecture** can transform raw data into meaningful insights, ensuring scalability, reliability, and performance across the data lifecycle.

---


