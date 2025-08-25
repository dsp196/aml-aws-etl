# Anti-Money Laundering (AML) ETL & Analytics Pipeline

## Project Overview
This project demonstrates an end-to-end data pipeline on the AWS stack using open-source IBM AML Synthetic Transactions Data.  
The raw dataset contained **431.17 million transaction rows**, highlighting the pipeline’s ability to handle large-scale financial data.  
The pipeline ingests, transforms, and curates financial transaction data, then builds analytical gold-layer tables for BI and ML modeling.  

The project mimics a real-world AML monitoring system, where suspicious financial patterns are detected and analyzed across multiple dimensions such as interbank flows, FX corridors, and transaction timing.  

---

## Objectives
- Transfer and ingest AML data from Google Cloud Storage (GCP) to AWS S3.  
- Process a **large-scale dataset (431M+ rows)** to demonstrate performance at scale.  
- Design an ETL pipeline with Bronze → Silver → Gold architecture.  
- Perform data cleansing and enrichment using PySpark on AWS EMR.  
- Apply partitioning and compression strategies for optimized query performance.  
- Use AWS Glue Crawler to catalog datasets and expose them to Athena.  
- Build aggregated gold-layer tables for advanced analytics.  

---

## Architecture

## 1. Data Ingestion
- **Source:** Original zipped dataset stored in **Google Cloud Storage (GCP Bucket)**.  
- **Process:**  
  - Data was decompressed in GCP.  
  - Transferred to **AWS S3** using AWS Data Sync.  
- **Destination:** Raw data stored in S3 bucket → `s3://ibmrawaml/raw/`.  
- **Scale:** Dataset contained **431.17 million rows** in raw CSV format.  

---

## 2. Bronze Layer (Raw Zone)
- Data is stored **as-is** in S3 (CSV format).  
- No transformations are applied at this stage.  
- Used primarily for **archival** and **data lineage tracking**.  
- Raw dataset volume: **431M+ transactions**.  

---

## 3. Silver Layer (Curated / Clean Zone)
- **Processing:**  
  - **AWS EMR (PySpark)** jobs parse the 431M+ raw rows.  
  - Timestamps are normalized.  
  - Partitioning applied by **Year (yr), Month (mo), Day (day)** for query optimization.  

- **Storage:**  
  - Written back to S3 in **Parquet + Snappy compression** (significantly reduced size vs raw CSV).  
  - Location: `s3://ibmrawaml/Spark_curated_day/`.  

- **AWS Glue Crawler:**  
  - Runs on the S3 curated folder.  
  - Auto-discovers schema.  
  - Registers metadata into **AWS Glue Data Catalog**.  
  - Table: `aml_silver_transactions`.  

---

## 4. Gold Layer (Analytics Zone)
Gold tables are built from Silver data using **aggregations and business rules** for dashboarding and ML.  
All transformations done in **PySpark (EMR)** and written to **S3 in Parquet**, cataloged in **Athena**.  

Gold tables include:  

1. **aml_gold_corridors** → Transaction corridors (sender ↔ receiver bank pairs).  
2. **aml_gold_daily** → Daily aggregated stats (txn count, laundering ratio, avg amounts).  
3. **aml_gold_fanout_idx** → Fan-out analysis (# of receivers per sender).  
4. **aml_gold_fx** → Currency corridor analytics.  
5. **aml_gold_fx_expanded** → Separated payment and receiving currencies for FX analysis.  
6. **aml_gold_hourly** → Hourly transaction distribution.  
7. **Date** → Calendar dimension for time-series reporting.  

---

## 5. Query Layer (Athena)
- All Gold/Silver tables queried directly via **Amazon Athena**.  
- Partition pruning ensures cost-effective queries even on **hundreds of millions of rows**.  
- Enables **ad-hoc SQL analysis** and **Power BI integration**.  

---

## 6. Dashboarding (Power BI)
- **Connection:** Power BI ↔ Athena using the **ODBC driver**.  
- **Pages created:**  
  - **Sender Network Analysis**  
  - **FX Corridor Analysis**  
  - **Hourly Activity**  
  - **Interbank Flow**  
  - **Daily Transactions**  
  - **Date dimension**
