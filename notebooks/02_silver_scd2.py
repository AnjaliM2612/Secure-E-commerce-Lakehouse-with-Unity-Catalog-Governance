
"""
02_silver_scd2.py - Silver Layer - SCD Type 2 + Deduplication + Time Travel
Bronze -> Silver with SCD Type 2 for customer address changes
"""

from pyspark.sql.functions import row_number, col, lit, current_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/bronze/customers/"
SILVER_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/silver/customers/"

# Read Bronze
bronze_df = spark.read.format("delta").load(BRONZE_CUSTOMERS)

# Deduplication - Keep latest per customer_id
window_spec = Window.partitionBy("customer_id").orderBy(col("updated_date").desc())
deduped_df = (bronze_df
    .withColumn("rn", row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# SCD Type 2 Implementation using Delta MERGE
# Silver table with scd columns: effective_date, end_date, is_current

# Create Silver table if not exists
try:
    silver_table = DeltaTable.forPath(spark, SILVER_CUSTOMERS)
except:
    # Initial load
    initial_df = deduped_df.withColumn("effective_date", current_date()).withColumn("end_date", lit("9999-12-31")).withColumn("is_current", lit(True))
    initial_df.write.format("delta").mode("overwrite").save(SILVER_CUSTOMERS)
    silver_table = DeltaTable.forPath(spark, SILVER_CUSTOMERS)

# MERGE for SCD2 - Detect address change
# Pseudocode for MERGE - Adapt to your schema
"""
MERGE INTO silver.customers AS target
USING deduped_df AS source
ON target.customer_id = source.customer_id AND target.is_current = true

WHEN MATCHED AND (target.address != source.address OR target.city != source.city) THEN
  UPDATE SET end_date = current_date(), is_current = false

WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, phone, address, city, effective_date, end_date, is_current)
  VALUES (source.customer_id, source.name, source.email, source.phone, source.address, source.city, current_date(), '9999-12-31', true)

# For changed records, insert new version
INSERT INTO silver.customers
SELECT source.customer_id, source.name, source.email, source.phone, source.address, source.city, current_date(), '9999-12-31', true
FROM deduped_df source
JOIN silver.customers target ON source.customer_id = target.customer_id
WHERE target.end_date = current_date() AND target.is_current = false
"""

# Time Travel Demo
# silver_table.history().show() # Audit history
# spark.read.format("delta").option("versionAsOf", 0).load(SILVER_CUSTOMERS).show() # Time Travel

# Deep Clone, Shallow Clone, VACUUM demo
# spark.sql(f"CREATE TABLE silver_customers_clone DEEP CLONE delta.`{SILVER_CUSTOMERS}`")
# spark.sql(f"VACUUM delta.`{SILVER_CUSTOMERS}` RETAIN 168 HOURS") # 7 days retention

print("SCD Type 2 Silver layer completed with Time Travel enabled")
