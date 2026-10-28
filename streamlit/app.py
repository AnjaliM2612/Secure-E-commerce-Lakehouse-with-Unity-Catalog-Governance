
"""
Streamlit Dashboard with Row-Level Filtering
Simulates Unity Catalog Row-Level Security
"""

import streamlit as st
import pandas as pd

st.set_page_config(page_title="E-commerce Revenue Dashboard", layout="wide")

st.title("Secure E-commerce Lakehouse - Revenue Dashboard")
st.markdown("**Row-Level Security Demo: Analyst sees only allowed city**")

# Simulate current_user from Unity Catalog ABAC
user_email = st.sidebar.selectbox("Select User (Simulates current_user())", 
    ["admin@company.com (ALL)", "analyst_pune@company.com (Pune only)", "viewer@company.com (Masked)"])

allowed_city = "ALL" if "admin" in user_email else "Pune" if "analyst_pune" in user_email else "Pune"

# Mock Gold data (Replace with Delta table read via Databricks SQL connector in real)
data = {
    "order_date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
    "city": ["Pune", "Mumbai", "Pune", "Bangalore"],
    "daily_revenue": [50000, 70000, 55000, 80000],
    "order_count": [100, 150, 110, 160]
}
df = pd.DataFrame(data)

# Row-level filtering
if allowed_city != "ALL":
    filtered_df = df[df["city"] == allowed_city]
    st.info(f"Row-Level Security Active: Showing only {allowed_city} data for {user_email}")
else:
    filtered_df = df
    st.success(f"Admin Access: Showing ALL cities for {user_email}")

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Total Revenue", f"₹{filtered_df['daily_revenue'].sum():,}")
col2.metric("Total Orders", f"{filtered_df['order_count'].sum():,}")
col3.metric("Avg Daily Revenue", f"₹{filtered_df['daily_revenue'].mean():,.0f}")

# Charts
st.bar_chart(filtered_df.set_index("city")["daily_revenue"])
st.dataframe(filtered_df)

# PII Masking Demo
st.subheader("PII Masking Demo")
customer_data = pd.DataFrame({
    "customer_id": ["CUST_000001", "CUST_000002"],
    "name": ["Amit Sharma", "Priya Patel"],
    "email": ["amit@example.com", "priya@example.com"],
    "phone": ["9876543210", "9876543211"],
    "city": ["Pune", "Mumbai"]
})

if "admin" in user_email:
    st.dataframe(customer_data)
    st.success("Admin sees unmasked PII")
else:
    masked = customer_data.copy()
    masked["email"] = "***@***.com"
    masked["phone"] = "**********"
    st.dataframe(masked)
    st.warning("Analyst/Viewer sees masked PII - Row-level + Column masking active")

st.markdown("---")
st.markdown("**Tech:** Azure Databricks, Delta Lake, Unity Catalog ABAC, Row-Level Security, PII Classification, Streamlit")
st.markdown("**GitHub:** github.com/anjali-lakehouse-governance | **Author:** Anjali Deore")
