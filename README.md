# Anti-Money Laundering (AML) ETL & Analytics Pipeline

## Project Overview
This project demonstrates an end-to-end data pipeline on the AWS stack using open-source IBM AML Synthetic Transactions Data.  
The pipeline ingests, transforms, and curates financial transaction data, then builds analytical gold-layer tables for BI and ML modeling.  

The project mimics a real-world AML monitoring system, where suspicious financial patterns are detected and analyzed across multiple dimensions such as interbank flows, FX corridors, and transaction timing.  

---

## Objectives
- Transfer and ingest AML data from Google Cloud Storage (GCP) to AWS S3.  
- Design an ETL pipeline with Bronze → Silver → Gold architecture.  
- Perform data cleansing and enrichment using PySpark on AWS EMR.  
- Apply partitioning and compression strategies for optimized query performance.  
- Use AWS Glue Crawler to catalog datasets and expose them to Athena.  
- Build aggregated gold-layer tables for advanced analytics.  

---

## Architecture

### Step 0 – Data Transfer (GCP → AWS)
- Original AML dataset was provided as a zipped CSV file on Google Cloud Storage (GCS).  
- Process:  
  1. Download dataset from GCS bucket.  
  2. Decompress (unzip) locally or in a GCP VM.  
  3. Upload extracted CSV files to AWS S3 (Bronze Layer).  

### Bronze Layer (Raw Data)
- Source: IBM AML transactions (CSV files).  
- Stored in: Amazon S3 (`s3://ibmrawaml/bronze/`).  
- Format: Raw CSV, unpartitioned.  
- Catalog: AWS Glue Crawler was used to automatically infer schema and register the dataset in the AWS Glue Data Catalog, making it queryable via Athena.  

### Silver Layer (Curated Transactions)
- Processing: PySpark on EMR  
- Transformations:  
  - Parse `timestamp` into `yr`, `mo`, `day` partitions  
  - Standardize schema (bank IDs, account IDs, currencies, amounts)  
  - Remove null/invalid rows  
- Storage: S3 in Parquet format with Snappy compression  
- Table: `aml_silver_transactions` (partitioned by `yr`, `mo`, `day`)  
- Catalog: Glue Crawler updates schema after each load.


## Gold Layer Tables

- **aml_gold_corridors** – Summarizes transaction volumes and values between sending and receiving banks to identify dominant corridors.  
- **aml_gold_daily** – Provides daily transaction KPIs such as total count, laundering ratio, and aggregated amounts.  
- **aml_gold_fanout_idx** – Flags accounts with multiple distinct recipients, used to detect potential layering/fan-out patterns.  
- **aml_gold_fx** – Captures foreign exchange transaction metrics, including total amounts, transaction counts, and spread indicators.  
- **aml_gold_fx_expanded** – Splits FX corridors into payment and receiving currencies for detailed cross-currency analysis.  
- **aml_gold_hourly** – Breaks down transactions by hour of the day to highlight intraday activity patterns.  
- **Date** – Calendar dimension table for enabling time-series analysis and building relationships across fact tables.  
