
"""
01_bronze_ingestion.py - Bronze Layer - Auto Loader Ingestion
AWS S3 Raw CSV/JSON/Parquet -> Delta Bronze
Run on: Azure Databricks / Databricks Community Edition
"""

from pyspark.sql.functions import input_file_name, current_timestamp

# Config - Change to your S3 path or DBFS
RAW_CUSTOMERS_PATH = "s3://anjali-ecommerce-lakehouse/raw/customers/"
RAW_ORDERS_PATH = "s3://anjali-ecommerce-lakehouse/raw/orders/"
RAW_PRODUCTS_PATH = "s3://anjali-ecommerce-lakehouse/raw/products/"

BRONZE_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/bronze/customers/"
BRONZE_ORDERS = "s3://anjali-ecommerce-lakehouse/bronze/orders/"
BRONZE_PRODUCTS = "s3://anjali-ecommerce-lakehouse/bronze/products/"

# For Community Edition without S3, use: "/tmp/ecommerce/raw/customers/"

# 1. Customers - CSV with Auto Loader
customers_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", BRONZE_CUSTOMERS + "_schema/")
    .option("header", "true")
    .load(RAW_CUSTOMERS_PATH)
    .withColumn("input_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

(customers_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", BRONZE_CUSTOMERS + "_checkpoint/")
    .option("mergeSchema", "true")
    .start(BRONZE_CUSTOMERS)
)

# 2. Orders - JSON
orders_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", BRONZE_ORDERS + "_schema/")
    .load(RAW_ORDERS_PATH)
    .withColumn("input_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

(orders_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", BRONZE_ORDERS + "_checkpoint/")
    .start(BRONZE_ORDERS)
)

# 3. Products - Parquet
products_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", BRONZE_PRODUCTS + "_schema/")
    .load(RAW_PRODUCTS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
)

(products_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", BRONZE_PRODUCTS + "_checkpoint/")
    .start(BRONZE_PRODUCTS)
)

# Schema Enforcement + File Dependency Check example
# For batch mode (if not streaming):
# customers_df = spark.read.format("csv").option("header","true").load(RAW_CUSTOMERS_PATH)
# Validations: count, null checks, schema enforcement
# customers_df.count() -> file dependency check
