#!/usr/bin/env python3
"""Generate and upload three test scenario CSV files for pipeline testing."""
import csv
import io
import boto3
import time
import json

BUCKET = "maxab-assessment-data"
REGION = "eu-north-1"
s3 = boto3.client("s3", region_name=REGION)

def make_order(order_id, customer_id, basket_value, payment_method="mobile_wallet"):
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "merchant_id": "MERCH-TEST",
        "basket_value": basket_value,
        "order_date": "2026-05-24",
        "payment_method": payment_method,
        "payment_status": "paid",
        "fulfilment_status": "pending",
    }

def make_customer(customer_id, tenure_days, total_orders, ltv, region="Cairo"):
    return {
        "customer_id": customer_id,
        "tenure_days": tenure_days,
        "total_orders": total_orders,
        "ltv": ltv,
        "region": region,
    }

def make_fraud_flag(order_id, fraud_score, reason="normal"):
    return {"order_id": order_id, "fraud_score": fraud_score, "flag_reason": reason}

def to_csv_bytes(rows, fieldnames):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()

def upload(key, data):
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    print(f"Uploaded s3://{BUCKET}/{key}")

# ── Scenario 1: New Customers ──────────────────────────────────────────────
nc_orders, nc_customers, nc_flags = [], [], []
for i in range(10):
    oid = f"TEST-NC-{i:03d}"
    cid = f"CUST-NC-{i:03d}"
    nc_orders.append(make_order(oid, cid, 800 + i * 200, "cash"))
    nc_customers.append(make_customer(cid, tenure_days=10 + i, total_orders=1 + i % 2, ltv=1000 + i * 200))
    nc_flags.append(make_fraud_flag(oid, 0.05 + i * 0.005, "normal"))

# ── Scenario 2: High Risk ──────────────────────────────────────────────────
hr_orders, hr_customers, hr_flags = [], [], []
for i in range(10):
    oid = f"TEST-HR-{i:03d}"
    cid = f"CUST-HR-{i:03d}"
    hr_orders.append(make_order(oid, cid, 55000 + i * 2000, "credit"))
    hr_customers.append(make_customer(cid, tenure_days=30 + i * 5, total_orders=3 + i, ltv=15000 + i * 1000))
    hr_flags.append(make_fraud_flag(oid, 0.72 + i * 0.02, "address_mismatch"))

# ── Scenario 3: Loyal Customers ────────────────────────────────────────────
ly_orders, ly_customers, ly_flags = [], [], []
for i in range(10):
    oid = f"TEST-LY-{i:03d}"
    cid = f"CUST-LY-{i:03d}"
    ly_orders.append(make_order(oid, cid, 3000 + i * 500, "mobile_wallet"))
    ly_customers.append(make_customer(cid, tenure_days=400 + i * 10, total_orders=25 + i, ltv=120000 + i * 5000))
    ly_flags.append(make_fraud_flag(oid, 0.03 + i * 0.003, "normal"))

order_fields = ["order_id", "customer_id", "merchant_id", "basket_value", "order_date", "payment_method", "payment_status", "fulfilment_status"]
cust_fields  = ["customer_id", "tenure_days", "total_orders", "ltv", "region"]
flag_fields  = ["order_id", "fraud_score", "flag_reason"]

scenarios = [
    ("new_customer", nc_orders, nc_customers, nc_flags),
    ("high_risk",    hr_orders, hr_customers, hr_flags),
    ("loyal",        ly_orders, ly_customers, ly_flags),
]

# Upload supporting data first (customers + fraud_flags get appended via separate keys)
for name, orders, customers, flags in scenarios:
    upload(f"raw/orders/test_{name}.csv",       to_csv_bytes(orders,    order_fields))
    upload(f"raw/customers/test_{name}.csv",    to_csv_bytes(customers, cust_fields))
    upload(f"raw/fraud_flags/test_{name}.csv",  to_csv_bytes(flags,     flag_fields))
    print(f"  {name}: {len(orders)} orders uploaded")
    time.sleep(1)

print("\nAll uploads complete.")
