-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Instacart E-Commerce Analytics
-- MAGIC **Databricks Notebook**  
-- MAGIC **ETL Layers:** Bronze → Silver → Gold  
-- MAGIC **Author:** Olaolu Motojesi  
-- MAGIC **Date:** 24 Feb 2026  
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
    aisle_id,
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
    department_id,
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
    order_id,
    product_id,
    add_to_cart_order,
    reordered
FROM bronze_order_products_prior
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL;

-- Preview
SELECT * FROM silver_order_products_prior LIMIT 5;

-- COMMAND ----------

-- 3.4 Silver: Orders (prior only)
CREATE OR REPLACE TABLE silver_orders AS
SELECT
    order_id,
    user_id,
    order_number,
    days_since_prior_order,        -- NULL for first order
     CASE 
        WHEN days_since_prior_order IS NULL THEN 1
        ELSE 0
    END AS is_first_order,        -- Flag first order
    order_dow,
    order_hour_of_day
FROM bronze_orders
WHERE eval_set = 'prior'
  AND user_id IS NOT NULL
  AND order_id IS NOT NULL;

-- Preview
SELECT * FROM silver_orders LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Untitled
-- 3.5 Silver: Products
CREATE OR REPLACE TABLE silver_products AS
SELECT
    CAST(product_id AS INT) AS product_id,
    product_name,
    TRY_CAST(aisle_id AS INT) AS aisle_id,
    TRY_CAST(department_id AS INT) AS department_id
FROM bronze_products
WHERE product_id IS NOT NULL
  AND product_name IS NOT NULL
  AND TRY_CAST(aisle_id AS INT)  IS NOT NULL
  AND TRY_CAST(department_id AS INT) IS NOT NULL;

-- Preview
SELECT * FROM silver_products LIMIT 5;



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


-- silver_orders: First Order Flag Check

SELECT DISTINCT is_first_order
FROM silver_orders;

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
-- MAGIC ## 5. GOLD LAYER – KPI Tables
-- MAGIC - Aggregate metrics for business insights
-- MAGIC - Metrics: Total Orders, Reorder Rate, Avg Orders per Customer, Repeat Customers, Top Products & Departments

-- COMMAND ----------

-- 5.1 Total Orders
CREATE OR REPLACE TABLE gold_total_orders AS
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM silver_orders;

SELECT * FROM gold_total_orders;

-- COMMAND ----------

-- 5.2 Reorder Rate
CREATE OR REPLACE TABLE gold_reorder_rate AS
SELECT SUM(reordered) * 1.0 / COUNT(*) AS reorder_rate
FROM silver_order_products_prior;

SELECT * FROM gold_reorder_rate;

-- COMMAND ----------

-- 5.3 Average Orders per Customer
CREATE OR REPLACE TABLE gold_avg_orders_per_customer AS
SELECT AVG(order_count) AS avg_orders_per_customer
FROM (
    SELECT user_id, COUNT(order_id) AS order_count
    FROM silver_orders
    GROUP BY user_id
) t;

SELECT * FROM gold_avg_orders_per_customer;

-- COMMAND ----------

-- 5.4 Repeat Customer Rate
CREATE OR REPLACE TABLE gold_repeat_customer_rate AS
SELECT SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_customer_rate
FROM (
    SELECT user_id, COUNT(order_id) AS order_count
    FROM silver_orders
    GROUP BY user_id
) t;

SELECT * FROM gold_repeat_customer_rate;

-- COMMAND ----------

-- 5.5 Top 20 Products by Order Volume
CREATE OR REPLACE TABLE gold_top_products AS
SELECT p.product_name, COUNT(*) AS order_volume
FROM silver_order_products_prior op
JOIN silver_products p
  ON op.product_id = p.product_id
GROUP BY p.product_name
ORDER BY order_volume DESC
LIMIT 20;

SELECT * FROM gold_top_products;

-- COMMAND ----------

-- 5.6 Top Departments by Order Volume
CREATE OR REPLACE TABLE gold_top_departments AS
SELECT d.department, COUNT(*) AS order_volume
FROM silver_order_products_prior op
JOIN silver_products p
  ON op.product_id = p.product_id
JOIN silver_departments d
  ON p.department_id = d.department_id
GROUP BY d.department
ORDER BY order_volume DESC;

SELECT * FROM gold_top_departments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Supplementary Behavioral Metrics
-- MAGIC - Average Days Between Orders
-- MAGIC - Average Time to Reorder Products
-- MAGIC - Median Days to Reorder per Product (excluding zeros and guaranteed median)

-- COMMAND ----------

-- 6.1 Average Days Between Orders
CREATE OR REPLACE TABLE gold_avg_days_between_orders AS
SELECT AVG(days_since_prior_order) AS avg_days_between_orders
FROM silver_orders
WHERE days_since_prior_order IS NOT NULL;

SELECT * FROM gold_avg_days_between_orders;

-- COMMAND ----------

-- 6.2a Average Days to Reorder Product (fixed)
CREATE OR REPLACE TABLE gold_avg_product_reorder_time AS
WITH user_product_orders AS (
    SELECT 
        o.user_id,
        op.product_id,
        o.order_id,
        o.days_since_prior_order
    FROM silver_orders o
    JOIN silver_order_products_prior op
      ON o.order_id = op.order_id
),
cumulative_days AS (
    SELECT *,
        SUM(COALESCE(TRY_CAST(days_since_prior_order AS DOUBLE), 0))
        OVER (PARTITION BY user_id ORDER BY order_id) AS cumulative_day
    FROM user_product_orders
),
reorders AS (
    SELECT *,
        LAG(cumulative_day) 
        OVER (PARTITION BY user_id, product_id ORDER BY order_id) AS previous_purchase_day
    FROM cumulative_days
)
SELECT AVG(cumulative_day - previous_purchase_day) AS avg_days_to_reorder_product
FROM reorders
WHERE previous_purchase_day IS NOT NULL;

SELECT * FROM gold_avg_product_reorder_time;

-- COMMAND ----------

-- 6.3 Median Days to Reorder per Product (excluding 0s, guaranteed median)
CREATE OR REPLACE TABLE gold_product_median_reorder AS
WITH user_product_orders AS (
    SELECT 
        o.user_id,
        op.product_id,
        o.order_id,
        o.days_since_prior_order
    FROM silver_orders o
    JOIN silver_order_products_prior op
      ON o.order_id = op.order_id
),
cumulative_days AS (
    SELECT *,
        SUM(COALESCE(TRY_CAST(days_since_prior_order AS DOUBLE), 0))
        OVER (PARTITION BY user_id ORDER BY order_id) AS cumulative_day
    FROM user_product_orders
),
reorders AS (
    SELECT *,
        LAG(cumulative_day) 
        OVER (PARTITION BY user_id, product_id ORDER BY order_id) AS previous_purchase_day
    FROM cumulative_days
),
valid_reorders AS (
    SELECT 
        product_id,
        cumulative_day - previous_purchase_day AS reorder_interval
    FROM reorders
    WHERE previous_purchase_day IS NOT NULL
      AND cumulative_day - previous_purchase_day > 0
)
SELECT
    p.product_id,
    p.product_name,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v.reorder_interval) AS median_days_to_reorder
FROM valid_reorders v
JOIN silver_products p
  ON v.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY median_days_to_reorder;

SELECT * FROM gold_product_median_reorder;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Validation of KPI Logic in Excel

-- COMMAND ----------

-- DBTITLE 1,Random sample for Excel validation
--7.1  Random sample of 15,000 order-product rows with user info
CREATE OR REPLACE TABLE excel_validation_sample AS
SELECT op.*,p.product_name,d.department,a.aisle,a.aisle_id,d.department_id, o.user_id, o.order_number, o.days_since_prior_order, o.is_first_order, o.order_dow, o.order_hour_of_day
FROM silver_order_products_prior op
JOIN silver_orders o
  ON op.order_id = o.order_id
JOIN silver_products p 
  ON  op.product_id = p.product_id
JOIN silver_departments d
  ON p.department_id = d.department_id
JOIN silver_aisles a
  ON p.aisle_id = a.aisle_id   
ORDER BY RAND()
LIMIT 15000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The SQL Gold Layer KPIs were computed on the full dataset, while the Excel validation used a 15,000-row random sample. Comparison shows that ratios, rankings, and trends (Reorder Rate, Top Product, Top Department, Average Days Between Orders) are consistent between SQL and the sample. Differences in user-level metrics such as Average Orders per Customer and Repeat Customer Rate are expected due to the small sample size and do not indicate errors. Overall, the Excel extract confirms that the SQL logic and KPI calculations are correct.....See excel workbook for details