# B142 Data Integration — Olist Brazilian E-Commerce Pipeline

**Student:** Daniela Rubio Sánchez
**Student ID:** GH1024883
**Module:** B142 Data Integration
**Institution:** Gisma University of Applied Sciences

**Video demo:** https://youtu.be/CoYDLGlEI_s
**Report:** file:///Users/danielarubio/Desktop/GISMA/Data%20Integration/Data%20Integration%20Report%20-%20GH1024883.pdf

---

## Overview

An end-to-end ETL pipeline built with **Apache Spark (PySpark)** on **Databricks Free Edition**, following the **medallion architecture pattern** (bronze/silver/gold). The pipeline ingests the Olist Brazilian E-Commerce dataset — 9 source files, ~100,000 orders — cleans and integrates it into a single analytical fact table, and answers six business questions using Spark SQL and Plotly visualizations.

---

## Repository Structure

```
notebooks/                  - five sequential PySpark notebooks implementing the pipeline
olist_data_integration_project.dbc  - full Databricks notebook archive (with outputs, importable directly)
report/                      - final project report (PDF)
README.md                    - this file
```

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle, 2021)
Download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The dataset consists of 9 CSV files (~1.5M rows total): orders, customers, sellers, products, order items, payments, reviews, geolocation, and a product category name translation table. To reproduce this project, download the dataset from the link above and upload the 9 CSVs to a Databricks Volume (see "How to Reproduce" below).

---

## Pipeline Stages

| Stage | Notebook | Description |
|---|---|---|
| Explore | `01_explore_raw_data.py` | Quick sanity check — read one CSV, count rows, inspect schema |
| Extract | `02_extract.py` | Load all 9 raw CSVs into Spark DataFrames (bronze tier), initial data quality checks (null counts) |
| Transform | `03_transform.py` | Clean and standardize data: timestamp parsing, deduplication, filter to delivered orders, latest-review-per-order (window function), product category translation, geolocation aggregation. Output saved as Parquet (silver tier) |
| Integrate | `04_load_integrate.py` | Aggregate items/payments per order, join all cleaned tables into a single `fact_orders` table, add derived columns (delivery time, delays, date parts). Output saved as partitioned Parquet (gold tier) |
| Analyze | `05_analytics_and_visualization.py` | Register fact table as a Spark SQL temp view, run 6 analytical SQL queries (one per business question), visualize results with Plotly |

---

## Tech Stack

- Apache Spark (PySpark, Spark SQL)
- Databricks Free Edition (serverless compute, Unity Catalog Volumes)
- Parquet (columnar storage, partitioned by year/month)
- Plotly (visualizations)

---

## How to Reproduce

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Download the dataset from Kaggle (link above).
3. Create a Volume `workspace.default.olist_raw` and upload the 9 Olist CSVs into it.
4. Create two empty Volumes: `workspace.default.olist_clean` and `workspace.default.olist_analytics`.
5. Import `olist_data_integration_project.dbc` into your Databricks workspace (Workspace → right-click → Import), or copy the contents of the `notebooks/` folder into 5 separate notebooks.
6. Run the notebooks in order: `01` → `02` → `03` → `04` → `05`.

---

## Business Questions & Key Findings

1. **How are sales evolving over time?** Revenue grew steadily through 2017–2018, with a pronounced spike (1.15M BRL) in November 2017 (Black Friday).
2. **Which product categories generate the most revenue?** Health & beauty leads, followed by watches/gifts and bed/bath/table — though high revenue doesn't always align with high review scores.
3. **Which Brazilian states are the biggest markets?** São Paulo (SP) alone accounts for ~37.4% of total revenue — a strong geographic concentration.
4. **How does delivery time relate to customer satisfaction?** A clear inverse relationship: 5-star orders averaged 10.6 delivery days vs. 20.1 days for 1-star orders.
5. **What payment methods do customers prefer?** Credit card dominates (75.9%), followed by Boleto (19.9%).
6. **Are sellers delivering on time?** On-time delivery stayed above 90% for most of the period, with a dip during the Black Friday peak in late 2017.

---

## Academic Note

This project was completed as part of the B142 Data Integration module assessment at Gisma University of Applied Sciences. The dataset is publicly available on Kaggle under its own license (see dataset page for details).
