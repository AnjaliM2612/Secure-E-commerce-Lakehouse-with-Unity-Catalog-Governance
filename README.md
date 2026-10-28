
# Secure E-commerce Lakehouse with Unity Catalog Governance & PII Classification

**Domain:** E-commerce Retail (Generic - No Healthcare)  
**Stack:** AWS S3 + Azure Databricks + Delta Lake + PySpark + Unity Catalog + Streamlit  
**Scale:** 100M+ rows synthetic | 40% runtime optimization | 60% manual effort reduced

## Architecture

```
                ┌─────────────────────────────────────────────┐
S3 Raw (CSV/JSON) → Bronze (Raw Delta) → Silver (Cleaned + SCD2) → Gold (KPIs)
                                ↓
                  Unity Catalog + ABAC (Master-table-driven)
                  Row-Level + Table-Level Security (3 roles)
                  LLM PII Classification (email/phone/address)
                                ↓
                  Streamlit Dashboard (Row-level filtered Revenue)
                                ↓
                  Databricks Workflows (Monitoring + Alerts)
```

## Key Features (Matches Deloitte/Amgen/ZS JDs)

### 1. Medallion Architecture + SCD Type 2
- **Bronze:** Auto Loader ingestion of CSV/JSON/Parquet customer, orders, products
- **Silver:** Deduplication with window functions, SCD Type 2 for customer address changes using Delta MERGE + Time Travel
- **Gold:** Daily revenue, Customer LTV, Churn, with Managed/External Tables

### 2. Unity Catalog Governance (USP - Rare)
- 3 Catalogs: raw, curated, gold
- ABAC: Master-table-driven authorization - Dynamic user-level access
- Row-Level Security: Admin sees all regions, Analyst sees only Pune, Viewer sees masked PII
- Table-Level Security: Gold revenue table only for admin/analyst
- PII Classification: LLM-based scanner auto-detects email/phone/address and applies masking - reduced manual tagging 60%

### 3. PySpark Tuning (40-60% runtime cut)
- **Before Tuning:** 45 mins for 100M rows
- **After Tuning:** 18 mins (60% cut)
- Techniques: Shuffle partitions (200 → 50), Broadcast Joins (small product table), Salting for skewed customer_id, AQE, Spark UI analysis, OOM fix

### 4. Monitoring + CI/CD
- Databricks Workflows with retries, SLA alerts via email
- GitHub CI/CD: Dev → Staging → Prod

## S3 Bucket Structure
```
s3://anjali-ecommerce-lakehouse/
├── raw/customers/ (CSV)
├── raw/orders/ (JSON)
├── raw/products/ (Parquet)
├── bronze/
├── silver/
└── gold/
```

## Results / Impact Numbers (For Resume)
- 100M+ rows ingested S3 → Gold
- 40% query performance improved via validation framework
- 60% manual effort reduced via Unity Catalog ABAC + PII auto-classification
- 60% runtime cut (45 mins → 18 mins) via PySpark tuning across 40+ pipelines (in project demo 1 pipeline)
- Zero manual PII review via LLM scanner

## Tech Stack for Resume
`Azure Databricks, Databricks, AWS S3, Delta Lake, Lakehouse, Delta Tables, Time Travel, Medallion Bronze/Silver/Gold, SCD Type 1/2, Delta MERGE, PySpark, Python, SQL, Spark SQL, Shuffle Partitions, Broadcast Joins, Salting, AQE, Unity Catalog, ABAC, Row-Level Security, Table-Level Security, PII Classification, Azure Key Vault, Databricks Workflows, Git, GitHub, CI/CD, Power BI, Streamlit`

## How to Run (Databricks Community Edition - Free)
1. Create Databricks Community Edition account
2. Create S3 bucket or use DBFS /mnt/data
3. Run notebooks in order: 01 → 02 → 03 → 04 → 05 → 06
4. Run Streamlit: streamlit run streamlit/app.py

## Notebooks Order
- 01_bronze_ingestion.py - Auto Loader
- 02_silver_scd2.py - SCD Type 2 + MERGE + Time Travel
- 03_gold_kpis.py - Revenue, LTV, Churn
- 04_unity_catalog_abac.py - ABAC + Row-level security
- 05_pii_classification_llm.py - LLM PII detection
- 06_pyspark_tuning.py - Tuning demo

## Author
Anjali Deore - Data Engineer - Pune - 4+ Years - Databricks Certified - anjalideore112@gmail.com
