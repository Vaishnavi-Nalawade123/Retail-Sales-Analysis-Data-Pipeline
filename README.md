# Retail Sales Analysis Data Pipeline

An **end-to-end batch data engineering project** that simulates a real-world retail analytics system using **Apache Airflow, Apache Spark (PySpark), Spark SQL, and HDFS**.

This project demonstrates how raw transactional data is ingested, cleaned, modeled, and aggregated into business-ready KPIs using a **Bronze–Silver–Gold data architecture**, orchestrated by a **single production-grade Airflow DAG**.

---

## Project Highlights

* End-to-end **daily batch analytics pipeline**
* **Industry-standard data lake architecture** (Bronze / Silver / Gold)
* **Fact & Dimension modeling** with correct grain
* Spark-based scalable transformations
* Airflow-orchestrated workflow with retries and alerts
* Automated **email reporting** from Gold layer metrics
* No heavy dependencies (Hive / RDBMS avoided for simplicity)

---

## High-Level Architecture

```
Synthetic Order Data (Daily)
        |
        v
Bronze Layer (Raw JSON)
        |
        v
Silver Layer (Fact & Dimensions)
        |
        v
Gold Layer (Business Aggregates)
        |
        v
Automated Email Report
```

---

## Technology Stack

| Component                  | Purpose                                                   |
| -------------------------- | --------------------------------------------------------- |
| **Apache Airflow**         | Workflow orchestration, scheduling, retries, email alerts |
| **Apache Spark (PySpark)** | Data cleansing, enrichment, aggregation                   |
| **Spark SQL**              | KPI generation and analytics                              |
| **HDFS**                   | Data lake storage                                         |
| **Python 3.10**            | Scripting and ETL logic                                   |

---

## Airflow DAG Design

The pipeline is orchestrated using **a single Airflow DAG** with clear task dependencies:

```
[FileSensor]
     ↓
[BashOperator: Upload to HDFS]
     ↓
[BashOperator: Bronze → Silver]
     ↓
[BashOperator: Silver → Gold]
     ↓
[EmailOperator]
```

### DAG Characteristics

* Runs **daily** (supports backfills)
* Retry and failure handling
* Partition-aware processing
* Email summary generated from Gold tables

---

## Data Lake Architecture

### Bronze Layer (Raw Data)

* Raw daily **JSON order transactions**
* Stored **as-is** (append-only)
* No transformations applied

```
/bronze/
 └── orders/
     └── order_date=YYYY-MM-DD/
```

---

### Silver Layer (Cleaned & Modeled)

Contains analytics-ready **Fact and Dimension tables**.

#### Fact Table – `fact_orders`

**Grain**
One product sold in one order, at one store, on one date.

**Columns**

* `order_id`
* `product_id`
* `store_id`
* `date_id`
* `quantity`
* `order_amount`

#### Dimension Tables

* `dim_category`
* `dim_product`
* `dim_store`
* `dim_date`

```
/silver/
 ├── fact_orders/
 ├── dim_category/
 ├── dim_product/
 ├── dim_store/
 └── dim_date/
```

**Transformations Applied**

* JSON explosion (order → order items)
* Data validation (quantity, price)
* Deduplication at fact grain
* Referential integrity with dimensions
* Date standardization via `dim_date`
* Stored as **Parquet**, partitioned by date

---

### Gold Layer (Business Aggregates)

Business-ready datasets used for reporting and analytics.

**Examples**

* Daily store revenue
* Top 5 products per store per day
* Daily category sales
* Daily overall sales
* Weekly weekday vs weekend trends

```
/gold/
 ├── gold_daily_store_revenue/
 ├── gold_daily_top_products_store/
 ├── gold_daily_category_sales_store/
 ├── gold_daily_overall_sales/
 └── gold_weekly_weekday_weekend_sales/
```

---

## Synthetic Data Strategy

Synthetic data is used **only to simulate a transactional source system**.

All transformation logic mirrors **real-world retail batch pipelines**, including:

* Quantity and price validation
* Deduplication logic
* Fact-dimension modeling
* Partition-aware processing
* Scalable Spark transformations

---

## Project Structure

```
Retail_sales_analysis_pipeline/
├── airflow/                # Airflow DAGs
├── spark/                  # PySpark ETL jobs
├── spark_sql/              # Spark SQL analytics
├── dim_data/               # Dimension CSV files
├── orders/                 # Generated daily JSON orders
├── gold_exports/           # Optional CSV exports
└── README.md
```

---

## Setup & Execution

### Environment Variables

```bash
export PROJECT_HOME=$HOME/Retail_sales_analysis_pipeline
export AIRFLOW_HOME=$PROJECT_HOME/airflow
```

### Requirements

* Python 3.10
* Apache Airflow 2.7.3
* Apache Spark
* PySpark
* Pandas
* PyArrow

---

## Business Use Cases Demonstrated

* Store-level performance tracking
* Product-level sales analysis
* Category-wise revenue trends
* Daily and weekly sales patterns
* KPI-ready datasets for dashboards

---

## Key Learning Outcomes

* Designing **fact & dimension models**
* Implementing **Bronze–Silver–Gold architecture**
* Writing **production-style Spark ETL**
* Building **Airflow DAGs for batch pipelines**
* Partition-aware data processing
* End-to-end data engineering workflow

---

## Summary

This project demonstrates a **real-world batch retail analytics pipeline** with:

* Correct dimensional modeling
* Scalable Spark transformations
* Reliable Airflow orchestration
* Business-ready analytics outputs

It is well-suited for:

* Data Engineering portfolios
* Interview discussions
* Hands-on learning of batch pipelines

