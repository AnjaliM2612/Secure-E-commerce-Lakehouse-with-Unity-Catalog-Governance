
"""
04_unity_catalog_abac.py - Unity Catalog Governance - ABAC + Row-Level Security
This is your USP - Rare skill - 5% engineers have this
Run on: Databricks with Unity Catalog enabled (or simulate with views for Community Edition)
"""

# For Databricks with Unity Catalog:
"""
-- 1. Create Catalogs
CREATE CATALOG IF NOT EXISTS raw;
CREATE CATALOG IF NOT EXISTS curated;
CREATE CATALOG IF NOT EXISTS gold;

-- 2. Master Table Driven Authorization
-- Create master table: user_roles (user_email, role, allowed_city)

CREATE TABLE gold.user_roles (
  user_email STRING,
  role STRING,
  allowed_city STRING
);

INSERT INTO gold.user_roles VALUES
('admin@company.com', 'admin', 'ALL'),
('analyst_pune@company.com', 'analyst', 'Pune'),
('viewer@company.com', 'viewer', 'Pune');

-- 3. ABAC - Row-Level Security using row filter function
CREATE FUNCTION gold.row_filter_city(city STRING)
RETURN IF(
  current_user() = 'admin@company.com' OR 
  EXISTS (SELECT 1 FROM gold.user_roles WHERE user_email = current_user() AND (allowed_city = 'ALL' OR allowed_city = city)),
  true,
  false
);

-- Apply row filter to gold table
ALTER TABLE gold.daily_revenue SET ROW FILTER gold.row_filter_city ON (city);

-- 4. Table-Level Security
GRANT SELECT ON TABLE gold.daily_revenue TO `analyst_pune@company.com`;
DENY SELECT ON TABLE gold.daily_revenue TO `viewer@company.com`; -- Viewer only sees masked view

-- 5. PII Masking - Column masking
CREATE FUNCTION gold.mask_email(email STRING)
RETURN CASE WHEN current_user() IN (SELECT user_email FROM gold.user_roles WHERE role = 'admin') THEN email ELSE '***@***.com' END;

ALTER TABLE curated.customers ALTER COLUMN email SET MASK gold.mask_email;

-- 6. For Community Edition without Unity Catalog - Simulate with Views:
"""

# Simulation for Community Edition (without Unity Catalog) using Views:
from pyspark.sql.functions import when, lit

SILVER_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/silver/customers/"
GOLD_REVENUE = "s3://anjali-ecommerce-lakehouse/gold/daily_revenue/"

# Simulate current_user
current_user_email = "analyst_pune@company.com" # Change to test
allowed_city = "Pune" # From user_roles master table

revenue_df = spark.read.format("delta").load(GOLD_REVENUE)

# Row-level filtering simulation
if allowed_city != "ALL":
    filtered_revenue = revenue_df.filter(f"city = '{allowed_city}'")
else:
    filtered_revenue = revenue_df

filtered_revenue.show()

# PII Masking simulation
customers_df = spark.read.format("delta").load(SILVER_CUSTOMERS).filter("is_current = true")

masked_customers = customers_df.withColumn("email_masked", 
    when(lit(current_user_email == "admin@company.com"), col("email")).otherwise(lit("***@***.com"))
)

masked_customers.show()

print("Unity Catalog ABAC + Row-Level Security simulation completed")
print("In real Databricks with Unity Catalog, this is enforced at catalog level, not via DataFrame filter")
