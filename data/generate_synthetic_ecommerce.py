
"""
generate_synthetic_ecommerce.py - Generate 100M rows synthetic e-commerce data
Run locally: python generate_synthetic_ecommerce.py
Output: CSV/JSON/Parquet for S3 upload
"""

import pandas as pd
import numpy as np
import random
from faker import Faker
import os

fake = Faker('en_IN')
Faker.seed(0)

# Config - Small demo: 100k rows (Scale to 100M by changing n)
n_customers = 10000
n_products = 500
n_orders = 100000

os.makedirs("data/raw/customers", exist_ok=True)
os.makedirs("data/raw/orders", exist_ok=True)
os.makedirs("data/raw/products", exist_ok=True)

print(f"Generating {n_customers} customers, {n_products} products, {n_orders} orders...")

# Customers
customers = []
cities = ["Pune", "Mumbai", "Bangalore", "Hyderabad", "Delhi", "Chennai"]
for i in range(n_customers):
    customers.append({
        "customer_id": f"CUST_{i:06d}",
        "name": fake.name(),
        "email": fake.email(),
        "phone": f"{random.randint(7000000000, 9999999999)}",
        "address": fake.street_address(),
        "city": random.choice(cities),
        "updated_date": fake.date_between(start_date='-2y', end_date='today').isoformat()
    })

pd.DataFrame(customers).to_csv("data/raw/customers/customers.csv", index=False)
print("Customers CSV generated")

# Products
products = []
categories = ["Electronics", "Clothing", "Books", "Home", "Grocery"]
for i in range(n_products):
    products.append({
        "product_id": f"PROD_{i:04d}",
        "product_name": fake.word() + " " + random.choice(categories),
        "category": random.choice(categories),
        "price": round(random.uniform(100, 10000), 2)
    })

pd.DataFrame(products).to_parquet("data/raw/products/products.parquet", index=False)
print("Products Parquet generated")

# Orders - JSON
orders = []
for i in range(n_orders):
    orders.append({
        "order_id": f"ORD_{i:07d}",
        "customer_id": f"CUST_{random.randint(0, n_customers-1):06d}",
        "product_id": f"PROD_{random.randint(0, n_products-1):04d}",
        "order_date": fake.date_between(start_date='-1y', end_date='today').isoformat(),
        "order_amount": round(random.uniform(100, 10000), 2),
        "quantity": random.randint(1, 5)
    })

pd.DataFrame(orders).to_json("data/raw/orders/orders.json", orient="records", lines=True)
print("Orders JSON generated")

print("Synthetic data generation completed - Upload data/raw/ to S3")
print("For 100M rows: Increase n_orders to 100000000 and use PySpark to generate (Pandas will OOM)")
