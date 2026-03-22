# Data Warehouse Data Catalog (Gold Layer)

## Overview
This data catalog documents the star schema implemented in the Gold Layer.

### Schema Design
- Dimensions:
  - dim_customers
  - dim_products
- Fact Table:
  - facts_sales

---

## 1. dim_customers

| Column | Type | Description |
|--------|------|-------------|
| customer_key | INT | Surrogate key (unique identifier) |
| customer_id | INT | Source system customer ID |
| customer_number | VARCHAR | Business key |
| first_name | VARCHAR | Customer first name |
| last_name | VARCHAR | Customer last name |
| gender | VARCHAR | Standardized gender |
| marital_status | VARCHAR | Marital status |
| birth_date | DATE | Date of birth |
| country | VARCHAR | Country of residence |

---

## 2. dim_products

| Column | Type | Description |
|--------|------|-------------|
| product_key | INT | Surrogate key |
| product_id | INT | Source product ID |
| product_number | VARCHAR | Business key |
| product_name | VARCHAR | Product name |
| category_id | VARCHAR | Category ID |
| category | VARCHAR | Product category |
| subcategory | VARCHAR | Product subcategory |
| maintenance | VARCHAR | Maintenance classification |
| product_line | VARCHAR | Product line |
| start_date | DATE | Effective start date |

---

## 3. facts_sales

| Column | Type | Description |
|--------|------|-------------|
| order_number | VARCHAR | Unique order ID |
| customer_key | INT | FK to dim_customers |
| product_key | INT | FK to dim_products |
| ship_date | DATE | Shipping date |
| due_date | DATE | Due date |
| quantity | INT | Quantity sold |
| price | DECIMAL | Unit price |
| sales_amount | DECIMAL | Total sales |

---

## Relationships

- facts_sales.customer_key → dim_customers.customer_key
- facts_sales.product_key → dim_products.product_key

---

## Business Use Cases

- Sales analysis by product and category
- Customer segmentation
- Revenue trends and forecasting
- Geographic performance analysis
