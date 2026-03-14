-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Instacart E-Commerce Analytics
-- MAGIC **Databricks Notebook**  
-- MAGIC **ETL Layers:** Bronze → Silver → Gold  
-- MAGIC **Author:** Olaolu Motojesi  
-- MAGIC **Date:** 13 Mar 2026  
-- MAGIC
-- MAGIC _This notebook demonstrates a layered ETL approach using the Instacart Online Grocery Dataset.  
-- MAGIC It prepares data for analytics and KPI calculation in a clean, scalable way._

-- COMMAND ----------

-- 1. CREATE DATABASE AND SET CONTEXT

CREATE DATABASE IF NOT EXISTS instacart;
USE instacart;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. BRONZE LAYER – Raw Data Ingestion
-- MAGIC
-- MAGIC - Keep all raw data as it is  
-- MAGIC - No cleaning or filtering yet  
-- MAGIC - Source files are CSVs uploaded in volume (`instacart_raw`) created in the workspace  
-- MAGIC
-- MAGIC ### Note  
-- MAGIC The `order_products_prior` dataset was added using Add Data → Create or Modify Table in the workspace.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %fs ls /Volumes/workspace/instacart/instacart_raw

-- COMMAND ----------

-- 2.1 Bronze Aisles
CREATE OR REPLACE TABLE bronze_aisles AS
SELECT *
FROM read_files(
  '/Volumes/workspace/instacart/instacart_raw/aisles.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- Preview
SELECT * FROM bronze_aisles LIMIT 5;

-- COMMAND ----------

-- 2.2 Bronze Departments
CREATE OR REPLACE TABLE bronze_departments AS
SELECT *
FROM read_files(
  '/Volumes/workspace/instacart/instacart_raw/departments.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- Preview
SELECT * FROM bronze_departments LIMIT 5;

-- COMMAND ----------

-- 2.3 Bronze Order Products Prior
-- Preview
SELECT * FROM bronze_order_products_prior LIMIT 5;

-- COMMAND ----------

-- 2.4 Bronze Orders
CREATE OR REPLACE TABLE bronze_orders AS
SELECT *
FROM read_files(
  '/Volumes/workspace/instacart/instacart_raw/orders.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- Preview
SELECT * FROM bronze_orders LIMIT 5;

-- COMMAND ----------

-- 2.5 Bronze Products
CREATE OR REPLACE TABLE bronze_products AS
SELECT *
FROM read_files(
  '/Volumes/workspace/instacart/instacart_raw/products.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);
-- Preview
SELECT * FROM bronze_products LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. SILVER LAYER – Cleaned and Validated Data
-- MAGIC - Remove NULLs from key identifiers
-- MAGIC - Add derived columns where needed
-- MAGIC - Prepare tables for analytics

-- COMMAND ----------

-- 3.1 Silver: Aisles
CREATE OR REPLACE TABLE silver_aisles AS
SELECT
    CAST(aisle_id AS STRING) AS aisle_id,
    aisle
FROM bronze_aisles
WHERE aisle_id IS NOT NULL
  AND aisle IS NOT NULL;

-- Preview
SELECT * FROM silver_aisles LIMIT 5;

-- COMMAND ----------

-- 3.2 Silver: Departments
CREATE OR REPLACE TABLE silver_departments AS
SELECT
    CAST(department_id AS STRING) AS department_id,
    department
FROM bronze_departments
WHERE department_id IS NOT NULL
  AND department IS NOT NULL;

-- Preview
SELECT * FROM silver_departments LIMIT 5;

-- COMMAND ----------

-- 3.3 Silver: Order Products Prior
CREATE OR REPLACE TABLE silver_order_products_prior AS
SELECT
    CAST(order_id AS STRING) AS order_id,
    CAST(product_id AS STRING) AS product_id,
    add_to_cart_order,
    reordered
FROM bronze_order_products_prior
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL;

-- Preview
SELECT * FROM silver_order_products_prior LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Untitled
-- 3.4 Silver: Orders (prior only)
CREATE OR REPLACE TABLE silver_orders AS

WITH cleaned_orders AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(user_id AS STRING) AS user_id,
        TRY_CAST(CAST(order_number AS DOUBLE) AS INT) AS order_number,
        TRY_CAST(CAST(days_since_prior_order AS DOUBLE) AS INT) AS days_since_prior_order_int,
        order_dow,
        TRY_CAST(CAST(order_hour_of_day AS DOUBLE) AS INT) AS order_hour_of_day_int,
        eval_set
    FROM bronze_orders
    WHERE eval_set = 'prior'
      AND user_id IS NOT NULL
      AND order_id IS NOT NULL
)

SELECT
    order_id,
    user_id,
    order_number,
    days_since_prior_order_int AS days_since_prior_order,
    
    -- Reorder cycle categories with explicit First Order label
    CASE
        WHEN days_since_prior_order_int IS NULL THEN 'First order'
        WHEN days_since_prior_order_int BETWEEN 0 AND 3 THEN '0-3 days'
        WHEN days_since_prior_order_int BETWEEN 4 AND 7 THEN '4-7 days'
        WHEN days_since_prior_order_int BETWEEN 8 AND 14 THEN '8-14 days'
        WHEN days_since_prior_order_int BETWEEN 15 AND 21 THEN '15-21 days'
        WHEN days_since_prior_order_int BETWEEN 22 AND 30 THEN '22-30 days'
    END AS reorder_cycle,
    
    -- Reorder tooltip labels with explicit First Order label
    CASE
        WHEN days_since_prior_order_int IS NULL THEN 'No prior order'
        WHEN days_since_prior_order_int BETWEEN 0 AND 3 THEN 'Immediate reorder'
        WHEN days_since_prior_order_int BETWEEN 4 AND 7 THEN 'Weekly cycle'
        WHEN days_since_prior_order_int BETWEEN 8 AND 14 THEN 'Bi-weekly'
        WHEN days_since_prior_order_int BETWEEN 15 AND 21 THEN 'Occasional'
        WHEN days_since_prior_order_int BETWEEN 22 AND 30 THEN 'Monthly'
     END AS reorder_tooltip,
    
    order_dow,
    
    -- Day of week labels
    CASE order_dow
        WHEN 0 THEN 'Sun'
        WHEN 1 THEN 'Mon'
        WHEN 2 THEN 'Tue'
        WHEN 3 THEN 'Wed'
        WHEN 4 THEN 'Thu'
        WHEN 5 THEN 'Fri'
        WHEN 6 THEN 'Sat'
        ELSE 'Unknown'
    END AS order_day,
    
    order_hour_of_day_int AS order_hour_of_day,
    
    -- Hour AM/PM labels
    CASE
        WHEN order_hour_of_day_int = 0 THEN '12 AM'
        WHEN order_hour_of_day_int BETWEEN 1 AND 11 THEN CONCAT(order_hour_of_day_int, ' AM')
        WHEN order_hour_of_day_int = 12 THEN '12 PM'
        WHEN order_hour_of_day_int BETWEEN 13 AND 23 THEN CONCAT(order_hour_of_day_int - 12, ' PM')
        ELSE 'Unknown'
    END AS order_hour_am_pm

FROM cleaned_orders;

-- Preview
SELECT * FROM silver_orders LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## 4. Silver Layer – Data Quality & Validation Checks
-- MAGIC
-- MAGIC This section validates cleaned datasets before KPI aggregation.
-- MAGIC
-- MAGIC Checks performed:
-- MAGIC - Null validation on key identifiers
-- MAGIC - Duplicate detection
-- MAGIC - Value integrity checks
-- MAGIC - Row count reconciliation (Bronze vs Silver)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.1 silver_aisles**

-- COMMAND ----------


-- silver_aisles: Null Checks

SELECT 
    COUNT(*) AS total_rows,
    COUNT(aisle_id) AS aisle_id_non_null,
    COUNT(aisle) AS aisle_non_null
FROM silver_aisles;

-- COMMAND ----------


-- silver_aisles: Duplicate Check

SELECT aisle_id, COUNT(*) AS cnt
FROM silver_aisles
GROUP BY aisle_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


-- silver_aisles: Row Count Reconciliation

SELECT 
    (SELECT COUNT(*) FROM bronze_aisles) AS bronze_count,
    (SELECT COUNT(*) FROM silver_aisles) AS silver_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.2 silver_departments**

-- COMMAND ----------


-- silver_departments: Null Checks

SELECT 
    COUNT(*) AS total_rows,
    COUNT(department_id) AS department_id_non_null,
    COUNT(department) AS department_non_null
FROM silver_departments;

-- COMMAND ----------


-- silver_departments: Duplicate Check

SELECT department_id, COUNT(*) AS cnt
FROM silver_departments
GROUP BY department_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


-- silver_departments: Row Count Reconciliation

SELECT 
    (SELECT COUNT(*) FROM bronze_departments) AS bronze_count,
    (SELECT COUNT(*) FROM silver_departments) AS silver_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.3 silver_order_products_prior**

-- COMMAND ----------


-- silver_order_products_prior: Null Checks

SELECT 
    COUNT(*) AS total_rows,
    COUNT(order_id) AS order_id_non_null,
    COUNT(product_id) AS product_id_non_null
FROM silver_order_products_prior;

-- COMMAND ----------


-- silver_order_products_prior: Duplicate Check

SELECT order_id, product_id, COUNT(*) AS cnt
FROM silver_order_products_prior
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


-- silver_order_products_prior: Reordered Value Check

SELECT DISTINCT reordered
FROM silver_order_products_prior;

-- COMMAND ----------


-- silver_order_products_prior: Row Count Reconciliation

SELECT 
    (SELECT COUNT(*) FROM bronze_order_products_prior) AS bronze_count,
    (SELECT COUNT(*) FROM silver_order_products_prior) AS silver_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.4 silver_orders**

-- COMMAND ----------


-- silver_orders: Null Checks

SELECT 
    COUNT(*) AS total_rows,
    COUNT(order_id) AS order_id_non_null,
    COUNT(user_id) AS user_id_non_null
FROM silver_orders;

-- COMMAND ----------


-- silver_orders: Duplicate Check

SELECT order_id, COUNT(*) AS cnt
FROM silver_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


-- silver_orders: Row Count Reconciliation

SELECT 
    (SELECT COUNT(*) FROM bronze_orders WHERE eval_set = 'prior') AS bronze_prior_count,
    (SELECT COUNT(*) FROM silver_orders) AS silver_count;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **4.5 silver_products**

-- COMMAND ----------


-- silver_products: Null Checks

SELECT 
    COUNT(*) AS total_rows,
    COUNT(product_id) AS product_id_non_null,
    COUNT(product_name) AS product_name_non_null,
    COUNT(aisle_id) AS aisle_id_non_null,
    COUNT(department_id) AS department_id_non_null
FROM silver_products;

-- COMMAND ----------


-- silver_products: Duplicate Check

SELECT product_id, COUNT(*) AS cnt
FROM silver_products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


-- silver_products: Referential Integrity Check

SELECT COUNT(*) AS invalid_aisles
FROM silver_products p
LEFT JOIN silver_aisles a
  ON p.aisle_id = a.aisle_id
WHERE a.aisle_id IS NULL;

-- COMMAND ----------


-- silver_products: Row Count Reconciliation

SELECT 
    (SELECT COUNT(*) FROM bronze_products) AS bronze_count,
    (SELECT COUNT(*) FROM silver_products) AS silver_count;

-- COMMAND ----------

SELECT p.product_id, p.product_name, p.aisle_id
FROM silver_products p
LEFT JOIN silver_aisles a
  ON p.aisle_id = a.aisle_id
WHERE a.aisle_id IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###5. GOLD LAYER – KPI & Analytics
-- MAGIC
-- MAGIC Curated fact & dimension tables for reporting, dashboards, and analytical insights.  
-- MAGIC
-- MAGIC __#  Workflow_
-- MAGIC
-- MAGIC Silver Tables → Gold Tables → SQL/Excel Validation → Power BI Dashboards  _
-- MAGIC
-- MAGIC
-- MAGIC ## 5.1 Core Tables
-- MAGIC
-- MAGIC **Fact:** `gold_fact_order_lines` – order line–level transactions  
-- MAGIC
-- MAGIC **Dimensions:**  
-- MAGIC - `gold_dim_customers` – customer attributes & segments  
-- MAGIC - `gold_dim_products` – product, aisle, & department info  
-- MAGIC
-- MAGIC ## 5.2 Key Metrics (SQL)
-- MAGIC
-- MAGIC Total Orders, Total customers, Average Orders per Customer, Reorder Rate, Repeat Customers   
-- MAGIC Orders by Day, Orders by Hour, Customers by Orders Segment, Top Products & Departments
-- MAGIC
-- MAGIC ## 5.3 Time Based Metrics
-- MAGIC Average Days to Reorder Products, Average Days Between Orders per customer
-- MAGIC
-- MAGIC ## 5.4 Key Metrics (EXCEL VALIDATION)
-- MAGIC
-- MAGIC ## 5.5 Excel Validation vs SQL Metrics
-- MAGIC

-- COMMAND ----------

-- 5.1 Core Tables
--FACT: Gold_fact_order_lines
CREATE OR REPLACE TABLE gold_fact_order_lines AS
SELECT
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_day,
    o.order_hour_of_day,
    o.order_hour_am_pm,
    o.days_since_prior_order,
    o.reorder_cycle,
    o.reorder_tooltip,

    op.product_id,
    op.add_to_cart_order,
    op.reordered
FROM silver_order_products_prior op
JOIN silver_orders o
  ON op.order_id = o.order_id;

-- COMMAND ----------

-- DIMENSION: Gold_dim_products
CREATE OR REPLACE TABLE gold_dim_products AS
SELECT
    p.product_id,
    p.product_name,
    COALESCE(a.aisle, 'Unknown') AS aisle_name,
    COALESCE(d.department, 'Unknown') AS department_name
FROM silver_products p
LEFT JOIN silver_aisles a
    ON p.aisle_id = a.aisle_id
LEFT JOIN silver_departments d
    ON p.department_id = d.department_id;


-- COMMAND ----------

-- DIMENSION: Gold_dim_customers
CREATE OR REPLACE TABLE gold_dim_customers AS
WITH customer_orders AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        COUNT(order_id) AS total_orders,
        MIN(order_id) AS first_order_id,
        MAX(order_id) AS last_order_id
    FROM silver_orders
    GROUP BY user_id
)
SELECT
    user_id,
    first_order_id,
    last_order_id,
    total_orders,
    CASE
        WHEN total_orders BETWEEN 1 AND 2 THEN 'First-time (1–2)'
        WHEN total_orders BETWEEN 3 AND 5 THEN 'New (3–5)'
        WHEN total_orders BETWEEN 6 AND 10 THEN 'Growing (6–10)'
        WHEN total_orders BETWEEN 11 AND 20 THEN 'Regular (11–20)'
        ELSE 'Power Users (20+)'
    END AS Customer_Segment,
    CASE
        WHEN total_orders BETWEEN 1 AND 2 THEN 0
        WHEN total_orders BETWEEN 3 AND 5 THEN 1
        WHEN total_orders BETWEEN 6 AND 10 THEN 2
        WHEN total_orders BETWEEN 11 AND 20 THEN 3
        ELSE 4
    END AS segment_sort,
    CASE
    WHEN total_orders BETWEEN 1 AND 2 
        THEN 'First-time customers with only 1–2 orders'
    WHEN total_orders BETWEEN 3 AND 5 
        THEN 'Customers starting to repeat purchases'
    WHEN total_orders BETWEEN 6 AND 10 
        THEN 'Customers becoming loyal with consistent ordering'
    WHEN total_orders BETWEEN 11 AND 20 
        THEN 'Highly engaged customers with frequent purchases'
    ELSE 
        'Top customers with very high order activity'
    END AS segment_description
FROM customer_orders;

-- COMMAND ----------

-- 5.2.1 Total Orders
CREATE OR REPLACE TABLE gold_total_orders AS
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM gold_fact_order_lines;

-- COMMAND ----------

-- 5.2.2 Avg Orders per Customer
CREATE OR REPLACE TABLE gold_avg_orders_per_customer AS
SELECT ROUND(AVG(total_orders),2) AS avg_orders_per_customer
FROM gold_dim_customers;

-- COMMAND ----------

-- 5.2.3 Total Customers
CREATE OR REPLACE TABLE gold_total_customers AS
SELECT COUNT(DISTINCT user_id) AS total_customers
FROM gold_fact_order_lines;


-- COMMAND ----------

-- 5.2.4 Percentage of repeat customers
CREATE OR REPLACE TABLE gold_pct_repeat_customers AS
SELECT 
    ROUND(100.0 * SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_repeat_customers
FROM gold_dim_customers;

-- COMMAND ----------

-- 5.2.5 Reorder Rate
CREATE OR REPLACE TABLE gold_reorder_rate AS
SELECT 
    ROUND(AVG(reordered)*100,2) AS reorder_rate
FROM gold_fact_order_lines;



-- COMMAND ----------

-- 5.2.6 Orders by Day of Week
CREATE OR REPLACE TABLE gold_orders_by_day_of_week AS
SELECT order_day,COUNT (DISTINCT order_id) AS total_orders
FROM gold_fact_order_lines
GROUP BY Order_day
ORDER BY total_orders DESC

-- COMMAND ----------

-- 5.2.7 Orders by Hour of Day
CREATE OR REPLACE TABLE gold_orders_by_hour_of_day AS
SELECT  order_hour_of_day AS 24hr_clock,
        order_hour_am_pm AS am_pm,
        COUNT (DISTINCT order_id) AS total_orders
FROM gold_fact_order_lines
GROUP BY order_hour_of_day,order_hour_am_pm
ORDER BY total_orders DESC

-- COMMAND ----------

-- 5.2.8 Customers by Order Segment
CREATE OR REPLACE TABLE gold_customers_by_order_segment AS
SELECT c.Customer_Segment,
       COUNT(DISTINCT f.user_id) AS total_customers
FROM gold_fact_order_lines f 
JOIN gold_dim_customers c 
ON f.user_id = c.user_id
GROUP BY c.Customer_Segment
ORDER BY total_customers DESC;

-- COMMAND ----------

-- 5.2.9 Orders by Reorder Cycle (Days Since Last Order)
-- Total orders grouped by reorder cycle (first order, 0-3 days, 4-7 days,8-14 days,15-21 days,22-30 days etc.)
CREATE OR REPLACE TABLE gold_orders_by_reorder_cycle AS
SELECT 
    reorder_cycle AS reorder_cycle_label,
    COUNT(*) AS total_orders
FROM gold_fact_order_lines
GROUP BY reorder_cycle
ORDER BY total_orders DESC
    

-- COMMAND ----------

-- 5.2.10 Top Products by Orders
CREATE OR REPLACE TABLE gold_top_products_by_orders AS
SELECT p.product_name,
      COUNT(*) AS total_orders
FROM gold_fact_order_lines f
JOIN gold_dim_products p 
ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_orders DESC
LIMIT 20;

-- COMMAND ----------

-- 5.2.11 Top Departments by Orders
CREATE OR REPLACE TABLE gold_top_departments_by_orders AS
SELECT p.department_name,
       COUNT(*) AS total_orders
FROM gold_fact_order_lines f
JOIN gold_dim_products p 
ON f.product_id = p.product_id
GROUP BY p.department_name
ORDER BY total_orders DESC
LIMIT 20;

-- COMMAND ----------

-- 5.3 Time-Based Metrics
--5.3.1 Avg days between orders per customer
CREATE OR REPLACE TABLE gold_avg_days_between_orders AS
SELECT ROUND(AVG(days_since_prior_order),2) AS avg_days_between_orders
FROM silver_orders
WHERE days_since_prior_order IS NOT NULL;



-- COMMAND ----------

-- 5.3.2 Avg days to reorder products
CREATE OR REPLACE TABLE gold_avg_days_reorder_cycle AS
SELECT ROUND(AVG(days_since_prior_order),2) AS avg_reorder_cycle
FROM gold_fact_order_lines
WHERE reordered = 1;

-- COMMAND ----------

-- 5.4 Key Metrics (Excel Validation)
-- Initial Excel validation proposed a 10–15K order sample. To better capture representative customer behaviour, I stratified by randomly selecting 5,000 customers and including all their orders.

CREATE OR REPLACE TABLE excel_validation_sample_co AS

WITH random_customers AS (
    SELECT user_id
    FROM gold_dim_customers
    ORDER BY RAND()
    LIMIT 5000
)

SELECT 
    f.*,
    c.customer_segment,
    c.segment_sort,
    c.segment_description,
    p.product_name,
    p.aisle_name,
    p.department_name
FROM random_customers rc
JOIN gold_dim_customers c
    ON rc.user_id = c.user_id
JOIN gold_fact_order_lines f
    ON rc.user_id = f.user_id
JOIN gold_dim_products p
    ON f.product_id = p.product_id;


SELECT *
FROM excel_validation_sample_co;  

-- COMMAND ----------

-- 5.5 Excel Validation vs SQL Metrics
-- Note: This table contains the KPI outputs previously calculated in SQL and copied into Excel for validation. It has been uploaded back into Databricks via Add Data → Create or Modify Table for reference in validation queries.

SELECT * FROM Excel_kpi_summary_report;
