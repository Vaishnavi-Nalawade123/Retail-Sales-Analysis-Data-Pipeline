# Retail-Sales-Analysis-Data-Pipeline

End-to-end data engineering project using Apache Airflow, Apache Spark (PySpark), Spark SQL, and HDFS.

# Project Overview

This project implements a production-style daily batch retail analytics pipeline. Synthetic transactional data is generated daily, ingested into a data lake, transformed into analytics-ready datasets, and aggregated into business KPIs with automated email reporting.

The pipeline follows an industry-standard Bronze–Silver–Gold data architecture and is orchestrated end-to-end using a single Apache Airflow DAG.

## Technology Responsibilities

  - **Airflow** – orchestration, scheduling, retries, email alerts
  - **HDFS** – data lake storage (Bronze / Silver / Gold)
  - **Apache Spark (PySpark)** – data cleansing, enrichment, aggregation
  - **Spark SQL** – business analytics and KPI generation

Heavy operational components such as Hive Metastore or relational databases are intentionally avoided to keep the pipeline simple, stable, and reproducible.

# High-Level Architecture

Synthetic Order Data (Daily)
        |
        v
Raw Orders (Bronze Layer)
        |
        v
Clean & Modeled Data (Silver Layer)
(Fact + Dimension Tables)
        |
        v
Daily Business Metrics (Gold Layer)
        |
        v
Automated Email Report
(to Manager / Team)

# Airflow DAG Design (Single DAG)

The entire pipeline is orchestrated using one Airflow DAG with clearly defined task dependencies.

[FileSensor]
     ↓
[BashOperator: upload to HDFS]
     ↓
[BashOperator : Bronze → Silver]
     ↓
[BashOperator: Silver → Gold]
     ↓
[EmailOperator]

# DAG Characteristics

  - Runs daily (with optional weekly aggregations)
  - Supports retries and backfills
  - Partition-aware processing
  - Email summary generated from Gold tables

Data Layers
Bronze Layer (Raw Data)

    Raw JSON order transactions (daily batches)
    Stored as-is in HDFS (append-only)

/bronze/
 └── orders/order_date=YYYY-MM-DD/

Silver Layer (Cleaned & Modeled)

    Exploded order items to achieve correct fact grain
    Data validation and deduplication
    Enriched with dimension tables
    Stored as Parquet and partitioned by date

Fact Table

fact_orders

Grain:
One product sold in one order, at one store, on one date

Columns:

    order_id
    product_id
    store_id
    date_id
    quantity
    order_amount

Dimensions

    dim_category
    dim_product
    dim_store
    dim_date

/silver/
 ├── fact_orders/
 ├── dim_category/
 ├── dim_product/
 ├── dim_store/
 └── dim_date/

Gold Layer (Business Aggregates)

Business-ready datasets materialized for reporting and reuse.

Examples:

    Daily store revenue
    Top 5 products per store per day
    Daily category sales
    Daily overall sales
    Weekly weekday vs weekend trends

/gold/
 ├── gold_daily_store_revenue/
 ├── gold_daily_top_products_store/
 ├── gold_daily_category_sales_store/
 ├── gold_daily_overall_sales/
 └── gold_weekly_weekday_weekend_sales/

Synthetic Data Strategy

Synthetic data is used only to simulate a transactional source system.
All validation, cleaning, and transformation logic mirrors real-world batch analytics pipelines.

Applied rules include:

    Quantity and price validation
    Referential integrity with dimensions
    Timestamp standardization using a date dimension
    Deduplication at fact grain

Project Structure

Retail_sales_analysis_pipeline/
├── airflow/                # Airflow DAG
├── spark/                  # Spark ETL jobs
├── spark_sql/              # Spark SQL analytics
├── dim_data/               # Dimension CSV files
├── orders/                 # Generated daily JSON orders
├── gold_exports/           # Optional CSV exports
└── README.md

Setup Summary
Environment Variables

export PROJECT_HOME=$HOME/Retail_sales_analysis_pipeline
export AIRFLOW_HOME=$PROJECT_HOME/airflow

Requirements

    Python 3.10
    Apache Airflow 2.7.3
    Apache Spark
    PySpark, Pandas, PyArrow

Summary

This project demonstrates a real-world batch-oriented retail analytics pipeline with correct fact and dimension modeling, scalable Spark transformations, and reliable Airflow orchestration.

