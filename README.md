# Instacart Customer Behaviour Analysis

This project analyses Instacart online grocery shopping data to uncover customer purchasing patterns, reorder behaviour, and product demand trends. The goal is to identify insights that can support better inventory planning, marketing strategies, and customer engagement.

## Dataset
Source: Kaggle – Instacart Online Grocery Shopping Dataset

The dataset contains over 3 million grocery orders from more than 200,000 customers, including information about products, departments, and reorder behaviour.

## Tools and Technologies
- Databricks SQL
- Microsoft Excel
- Power BI
- GitHub

## Project Architecture

The project follows the **Databricks Medallion Architecture** to structure the data pipeline from raw data ingestion to analytical insights.

![Project Architecture](architecture/instacart_architecture.png)

Data flows through three layers:

**Bronze Layer**  
Raw Instacart datasets are ingested and stored without modification to preserve the original source data.

**Silver Layer**  
Basic data cleaning and preparation are performed to ensure data consistency and usability.

**Gold Layer**  
Business-ready tables are created through joins and aggregations across orders, products, and departments to generate key metrics and support analysis.

## Analysis Workflow

1. Raw Instacart data was ingested into Databricks.
2. Data was processed through the Bronze, Silver, and Gold layers following the Medallion Architecture.
3. Customer behaviour metrics and KPIs were analysed using Databricks SQL.
4. Key metrics were validated and visualised using Microsoft Excel.
5. Insights were presented through an interactive Power BI dashboard.

## Key Insights

- Customers show strong repeat purchasing behaviour.
- Most customers reorder within a **4–7 day cycle**, reflecting weekly grocery shopping habits.
- **Bananas** are the most frequently ordered product.
- The **Produce department** drives the highest order volume.
- Orders peak between **10 AM and 3 PM**, with **Sundays and Mondays** showing the highest activity.

## Repository Structure

instacartportfolioproject  
│  
├── report  
│   └── Instacart_Ecommerce_Analytics_Report.pdf 
│  
├── excel  
│   └── Instacart_Analysis_Workbook.xlsx  
│  
├── dashboard  
│   └── powerbi_dashboard.png (included as appendix in Instacart_Ecommerce_Analytics_Report) 
│  
├── sql  
│   └── instacart_analysis_queries.sql  
│  
├── architecture  
│   └── instacart_architecture.png  
│  
└── README.md  

## Project Outputs

The project includes:

- SQL queries used for analysis
- Excel workbook used for KPI validation and charts
- Power BI dashboard visualising key insights
- Final analytical report summarising findings and recommendations

## Author

Motojesi Olaolu  
Data Analytics Project
