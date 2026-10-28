
"""
03_gold_kpis.py - Gold Layer - KPIs - Revenue, LTV, Churn
Silver -> Gold Aggregations
"""

SILVER_CUSTOMERS = "s3://anjali-ecommerce-lakehouse/silver/customers/"
SILVER_ORDERS = "s3://anjali-ecommerce-lakehouse/silver/orders/"
GOLD_REVENUE = "s3://anjali-ecommerce-lakehouse/gold/daily_revenue/"
GOLD_LTV = "s3://anjali-ecommerce-lakehouse/gold/customer_ltv/"

from pyspark.sql.functions import sum as _sum, count, avg, datediff, current_date, col

customers = spark.read.format("delta").load(SILVER_CUSTOMERS).filter("is_current = true")
orders = spark.read.format("delta").load(SILVER_ORDERS)

# Gold 1: Daily Revenue by Region/City
daily_revenue = (orders
    .join(customers, "customer_id")
    .groupBy("order_date", "city")
    .agg(_sum("order_amount").alias("daily_revenue"), count("order_id").alias("order_count"))
)

daily_revenue.write.format("delta").mode("overwrite").save(GOLD_REVENUE)

# Gold 2: Customer LTV
customer_ltv = (orders
    .groupBy("customer_id")
    .agg(_sum("order_amount").alias("lifetime_value"), count("order_id").alias("total_orders"), avg("order_amount").alias("avg_order_value"))
)

customer_ltv.write.format("delta").mode("overwrite").save(GOLD_LTV)

# Managed vs External Table example
# spark.sql("CREATE TABLE gold.daily_revenue USING DELTA LOCATION 's3://anjali-ecommerce-lakehouse/gold/daily_revenue/'")
# spark.sql("CREATE TABLE gold.customer_ltv (customer_id STRING, lifetime_value DOUBLE) USING DELTA")

print("Gold KPIs completed - Revenue and LTV")
