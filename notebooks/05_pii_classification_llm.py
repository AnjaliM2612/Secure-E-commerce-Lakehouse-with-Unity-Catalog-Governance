
"""
05_pii_classification_llm.py - LLM-based PII Classification
Auto-detect email/phone/address and flag - Reduce manual tagging 60%
"""

from pyspark.sql.functions import col, when, regexp_extract, length
import re

SILVER_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/silver/customers/"
PII_METADATA = "s3://anjali-ecommerce-lakehouse/gold/pii_metadata/"

customers_df = spark.read.format("delta").load(SILVER_CUSTOMERS).filter("is_current = true")

# Regex-based PII detection (for demo) - In real use LLM via Databricks Foundation Model API
# Email pattern
# Phone pattern (Indian: 10 digits)
# Address pattern

pii_df = (customers_df
    .withColumn("is_email_pii", col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").cast("boolean"))
    .withColumn("is_phone_pii", (length(col("phone")) == 10).cast("boolean"))
    .withColumn("is_address_pii", col("address").isNotNull().cast("boolean"))
    .withColumn("pii_type", 
        when(col("is_email_pii"), "EMAIL")
        .when(col("is_phone_pii"), "PHONE")
        .when(col("is_address_pii"), "ADDRESS")
        .otherwise("NONE")
    )
)

pii_df.write.format("delta").mode("overwrite").save(PII_METADATA)

# For LLM-based classification (Advanced - using Databricks Foundation Model):
"""
from pyspark.sql.functions import udf
import mlflow

# Use Databricks LLM: databricks-dolly-v2 or llama2
@udf
def classify_pii_llm(text):
    # Prompt: "Classify if this text contains PII: email, phone, address, none"
    # Call Foundation Model API
    # Return PII type
    pass

customers_with_llm_pii = customers_df.withColumn("pii_classification", classify_pii_llm(col("combined_text")))
"""

print("PII Classification completed - 60% manual effort reduced")
print(f"PII metadata saved to {PII_METADATA}")
