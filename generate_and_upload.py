"""
Generates a realistic 200k-order dummy dataset for a B2B e-commerce
ordering & payments platform (MaxAB-style, Egypt market).

Tables:
  orders       — one row per order, basket_value = sum of line items
  order_items  — one row per SKU line within an order
  customers    — one row per customer
  fraud_flags  — flagged orders (~3 %)
"""

import uuid
from datetime import date, timedelta

import boto3
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)

# ── constants ────────────────────────────────────────────────────────────────

N_ORDERS    = 200_000
N_CUSTOMERS = 12_000
N_MERCHANTS = 400

BUCKET = "maxab-assessment-data"

REGIONS        = ["Cairo","Giza","Alexandria","Delta","Upper Egypt",
                  "Red Sea","Sinai","Canal Zone","Beheira","Sharqia"]
REGION_WEIGHTS = [0.30, 0.18, 0.14, 0.12, 0.10, 0.04, 0.03, 0.03, 0.03, 0.03]

PAYMENT_METHODS  = ["cash","credit_card","mobile_wallet","bank_transfer","BNPL"]
PAYMENT_WEIGHTS  = [0.45,  0.20,         0.18,           0.10,           0.07]

PAYMENT_STATUSES             = ["paid","pending","failed","refunded"]
PAYMENT_STATUS_WEIGHTS_NORMAL = [0.82,  0.09,    0.06,    0.03]
PAYMENT_STATUS_WEIGHTS_FRAUD  = [0.30,  0.25,    0.35,    0.10]

FULFILMENT_STATUSES             = ["delivered","in_transit","processing","cancelled","returned"]
FULFILMENT_WEIGHTS_NORMAL = [0.75, 0.10, 0.06, 0.06, 0.03]
FULFILMENT_WEIGHTS_FRAUD  = [0.30, 0.10, 0.05, 0.40, 0.15]

# SKU pool — category prefix encodes pricing & qty behaviour
SKU_CATEGORIES = {
    "FMCG":   [f"FMCG-{i:04d}"   for i in range(1, 151)],
    "BVRG":   [f"BVRG-{i:04d}"   for i in range(1,  51)],
    "DAIRY":  [f"DAIRY-{i:04d}"  for i in range(1,  31)],
    "CLEAN":  [f"CLEAN-{i:04d}"  for i in range(1,  41)],
    "SNACK":  [f"SNACK-{i:04d}"  for i in range(1,  61)],
    "PHARMA": [f"PHARMA-{i:04d}" for i in range(1,  21)],
}
ALL_SKUS = np.array([s for skus in SKU_CATEGORIES.values() for s in skus])

# Unit price (EGP) and quantity ranges per category
SKU_PRICE_RANGE = {
    "FMCG":   (25,  350),
    "BVRG":   (15,  120),
    "DAIRY":  (20,  200),
    "CLEAN":  (30,  280),
    "SNACK":  (8,    90),
    "PHARMA": (50,  750),
}
SKU_QTY_RANGE = {
    "FMCG":   (5,  60),
    "BVRG":   (6,  96),   # cases
    "DAIRY":  (6,  48),
    "CLEAN":  (3,  30),
    "SNACK":  (10, 120),
    "PHARMA": (1,  20),
}

# Item-count distribution: peaks at 3–7, long tail to 15
# raw weights for counts 1..15
_raw_item_w = np.array([2, 5, 12, 15, 14, 11, 8, 5, 4, 3, 2, 1.5, 1, 0.8, 0.7])
ITEM_COUNT_PROBS = _raw_item_w / _raw_item_w.sum()
ITEM_COUNTS      = np.arange(1, 16)

FRAUD_REASONS = [
    "velocity_spike", "unusual_basket_composition", "payment_mismatch",
    "new_account_large_order", "multiple_failed_attempts",
    "address_mismatch", "device_fingerprint_anomaly", "off_hours_order",
]

START_DATE      = date(2023, 1, 1)
END_DATE        = date(2025, 4, 30)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


# ── helpers ──────────────────────────────────────────────────────────────────

def random_dates(n: int) -> list[date]:
    days = rng.integers(0, DATE_RANGE_DAYS, size=n)
    dates = [START_DATE + timedelta(days=int(d)) for d in days]
    # Ramadan uplift: resample ~40 % of Mar/Apr dates to add seasonal peak
    for i, d in enumerate(dates):
        if d.month in (3, 4) and rng.random() < 0.4:
            dates[i] = START_DATE + timedelta(days=int(rng.integers(0, DATE_RANGE_DAYS)))
    return dates


# ── 1. customers ─────────────────────────────────────────────────────────────
print("Generating customers …")

customer_ids = [f"CUST-{str(uuid.uuid4())[:8].upper()}" for _ in range(N_CUSTOMERS)]
customers_df = pd.DataFrame({
    "customer_id":  customer_ids,
    "tenure_days":  rng.integers(1, 900, size=N_CUSTOMERS),
    "total_orders": np.maximum(1, rng.poisson(lam=8, size=N_CUSTOMERS)),
    "ltv":          np.round(rng.lognormal(8.5, 1.1, size=N_CUSTOMERS), 2),
    "region":       rng.choice(REGIONS, size=N_CUSTOMERS, p=REGION_WEIGHTS),
})

# ── 2. order metadata (no sku, no basket_value yet) ──────────────────────────
print("Generating order metadata …")

merchant_ids = [f"MERCH-{str(uuid.uuid4())[:6].upper()}" for _ in range(N_MERCHANTS)]

N_FRAUD  = int(N_ORDERS * 0.025)
N_NORMAL = N_ORDERS - N_FRAUD
is_fraud = np.array([False] * N_NORMAL + [True] * N_FRAUD)
rng.shuffle(is_fraud)

order_ids = [f"ORD-{str(uuid.uuid4())[:10].upper()}" for _ in range(N_ORDERS)]

pay_status = np.empty(N_ORDERS, dtype=object)
pay_status[~is_fraud] = rng.choice(PAYMENT_STATUSES, (~is_fraud).sum(), p=PAYMENT_STATUS_WEIGHTS_NORMAL)
pay_status[ is_fraud] = rng.choice(PAYMENT_STATUSES,   is_fraud.sum(),  p=PAYMENT_STATUS_WEIGHTS_FRAUD)

ful_status = np.empty(N_ORDERS, dtype=object)
ful_status[~is_fraud] = rng.choice(FULFILMENT_STATUSES, (~is_fraud).sum(), p=FULFILMENT_WEIGHTS_NORMAL)
ful_status[ is_fraud] = rng.choice(FULFILMENT_STATUSES,   is_fraud.sum(),  p=FULFILMENT_WEIGHTS_FRAUD)

# ── 3. order_items ────────────────────────────────────────────────────────────
print("Generating order_items …")

# Fraud orders skew slightly larger (unusual_basket_composition signal)
item_counts = np.empty(N_ORDERS, dtype=int)
item_counts[~is_fraud] = rng.choice(ITEM_COUNTS, (~is_fraud).sum(), p=ITEM_COUNT_PROBS)
# fraud: shift distribution right by 2 positions, cap at 15
fraud_probs_shifted = np.roll(ITEM_COUNT_PROBS, 2)
fraud_probs_shifted[:2] = 0
fraud_probs_shifted /= fraud_probs_shifted.sum()
item_counts[is_fraud] = rng.choice(ITEM_COUNTS, is_fraud.sum(), p=fraud_probs_shifted)

# Expand order IDs to item rows
order_id_expanded = np.repeat(order_ids, item_counts)
total_items = len(order_id_expanded)

# Assign a unique SKU per line within each order.
# Strategy: for each distinct item-count n, batch all orders of that size,
# draw an (n_orders × n_skus) random matrix, argsort each row, take first n.
# This guarantees no duplicate SKU within an order without a Python loop.
sku_col  = np.empty(total_items, dtype=object)
pos      = np.concatenate([[0], np.cumsum(item_counts)])   # start index per order

for n in ITEM_COUNTS:
    batch_idx = np.where(item_counts == n)[0]
    if len(batch_idx) == 0:
        continue
    n_batch = len(batch_idx)
    # partial Fisher-Yates via argsort of uniform randoms
    rand_mat = rng.random((n_batch, len(ALL_SKUS)))
    chosen   = np.argsort(rand_mat, axis=1)[:, :n]           # (n_batch, n)
    selected = ALL_SKUS[chosen]                               # (n_batch, n)
    for j, oi in enumerate(batch_idx):
        sku_col[pos[oi]:pos[oi] + n] = selected[j]

# Vectorised price + qty per category
categories  = np.array([s.split("-")[0] for s in sku_col])
unit_prices = np.zeros(total_items)
quantities  = np.zeros(total_items, dtype=int)

for cat, (pmin, pmax) in SKU_PRICE_RANGE.items():
    m = categories == cat
    if m.any():
        unit_prices[m] = np.round(rng.uniform(pmin, pmax, m.sum()), 2)

for cat, (qmin, qmax) in SKU_QTY_RANGE.items():
    m = categories == cat
    if m.any():
        quantities[m] = rng.integers(qmin, qmax + 1, m.sum())

line_totals = np.round(unit_prices * quantities, 2)

order_items_df = pd.DataFrame({
    "order_id":   order_id_expanded,
    "sku_id":     sku_col,
    "quantity":   quantities,
    "unit_price": unit_prices,
    "line_total": line_totals,
})

# ── 4. finalise orders (basket_value = Σ line_total) ─────────────────────────
basket_by_order = (
    order_items_df.groupby("order_id")["line_total"]
    .sum()
    .round(2)
    .rename("basket_value")
)

orders_df = pd.DataFrame({
    "order_id":          order_ids,
    "customer_id":       rng.choice(customer_ids,  N_ORDERS),
    "merchant_id":       rng.choice(merchant_ids,  N_ORDERS),
    "basket_value":      basket_by_order.reindex(order_ids).values,
    "order_date":        random_dates(N_ORDERS),
    "payment_method":    rng.choice(PAYMENT_METHODS, N_ORDERS, p=PAYMENT_WEIGHTS),
    "payment_status":    pay_status,
    "fulfilment_status": ful_status,
})

# ── 5. fraud_flags ────────────────────────────────────────────────────────────
print("Generating fraud flags …")

borderline_mask = (~is_fraud) & (rng.random(N_ORDERS) < 0.005)

fraud_scores = np.empty(N_ORDERS)
fraud_scores[is_fraud]       = rng.beta(7,   2, is_fraud.sum())
fraud_scores[borderline_mask]= rng.beta(3,   4, borderline_mask.sum())
clean_mask                   = ~is_fraud & ~borderline_mask
fraud_scores[clean_mask]     = rng.beta(1.5, 8, clean_mask.sum())
fraud_scores = np.clip(np.round(fraud_scores, 4), 0, 1)

flag_mask = is_fraud | borderline_mask
fraud_df = pd.DataFrame({
    "order_id":    np.array(order_ids)[flag_mask],
    "fraud_score": fraud_scores[flag_mask],
    "flag_reason": rng.choice(FRAUD_REASONS, flag_mask.sum()),
})

# ── 6. save CSVs ──────────────────────────────────────────────────────────────
print("Saving CSVs …")
orders_df.to_csv("orders.csv",           index=False)
order_items_df.to_csv("order_items.csv", index=False)
customers_df.to_csv("customers.csv",     index=False)
fraud_df.to_csv("fraud_flags.csv",       index=False)
print(f"  orders.csv       → {len(orders_df):,} rows")
print(f"  order_items.csv  → {len(order_items_df):,} rows")
print(f"  customers.csv    → {len(customers_df):,} rows")
print(f"  fraud_flags.csv  → {len(fraud_df):,} rows")

# ── 7. upload to S3 ───────────────────────────────────────────────────────────
print(f"\nUploading to s3://{BUCKET}/ …")
s3 = boto3.client("s3")

uploads = {
    "orders.csv":       "raw/orders/orders.csv",
    "order_items.csv":  "raw/order_items/order_items.csv",
    "customers.csv":    "raw/customers/customers.csv",
    "fraud_flags.csv":  "raw/fraud_flags/fraud_flags.csv",
}
for local, key in uploads.items():
    s3.upload_file(local, BUCKET, key, ExtraArgs={"ContentType": "text/csv"})
    print(f"  ✓  s3://{BUCKET}/{key}")

print("\nDone.")
