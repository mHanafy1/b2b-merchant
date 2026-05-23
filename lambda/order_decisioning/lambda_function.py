"""
Lambda 1 — Order Decisioning  (v3)
Trigger: s3:ObjectCreated:* on prefix raw/orders/

Five-factor risk model
──────────────────────
  base_risk = 0.55 × fraud_score
            + 0.25 × min(basket_value / basket_p95, 1)
            + 0.20 × (1 − min(ltv / ltv_p95, 1))

  loyalty_discount      = clip((tenure_days / 365) × (1 − customer_fraud_rate), 0, 0.15)

  manual_rate_discount  = clip((approval_rate − 0.80) / 0.20 × 0.10, 0, 0.10)
                          applied only when customer has ≥ 5 manual reviews AND approval_rate > 0.80

  raw_risk   = clip(base_risk − loyalty_discount − manual_rate_discount, 0, 1)
  risk_score = max(raw_risk, 0.20)   ← 0.20 floor for new customers only

New-customer rules (tenure_days < 30 AND total_orders < 3 AND approved_orders < 3):
  • Cannot auto-approve — minimum decision is manual-review
  • basket_value < 5 000 AND fraud_score < 0.15  →  fast-track-review  (flagged in decision_reason)
  • Lifted after 3 cumulative approved orders in the actions table

Caps (basket_p95, ltv_p95) are computed at invocation time from the reference dataset.

Environment variables:
  S3_BUCKET            – bucket (default: maxab-assessment-data)
  CUSTOMERS_KEY        – raw/customers/customers.csv
  FRAUD_FLAGS_KEY      – raw/fraud_flags/fraud_flags.csv
  ORDERS_HISTORY_KEY   – raw/orders/orders.csv
  ACTIONS_HISTORY_KEY  – raw/actions/actions_reference.csv
"""

import io
import logging
import os
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET           = os.environ.get("S3_BUCKET",           "maxab-assessment-data")
CUSTOMERS_KEY       = os.environ.get("CUSTOMERS_KEY",       "raw/customers/customers.csv")
FRAUD_FLAGS_KEY     = os.environ.get("FRAUD_FLAGS_KEY",     "raw/fraud_flags/fraud_flags.csv")
ORDERS_HISTORY_KEY  = os.environ.get("ORDERS_HISTORY_KEY",  "raw/orders/orders.csv")
ACTIONS_HISTORY_KEY = os.environ.get("ACTIONS_HISTORY_KEY", "raw/actions/actions_reference.csv")
DECISIONS_PREFIX    = "decisions/"

# Risk thresholds
RISK_AUTO_APPROVE = 0.25
RISK_DECLINE      = 0.55

# Discount caps
LOYALTY_CAP           = 0.15
MANUAL_RATE_CAP       = 0.10
MANUAL_RATE_THRESHOLD = 0.80   # approval rate must exceed this
MANUAL_RATE_MIN_COUNT = 5      # minimum historical manual reviews required

# Fraud-rate threshold
FRAUD_RATE_THRESH = 0.50

# New-customer thresholds
NEW_CUST_TENURE_DAYS   = 30
NEW_CUST_MAX_ORDERS    = 3
NEW_CUST_MIN_APPROVED  = 3     # approved orders needed to lift the flag
NEW_CUST_RISK_FLOOR    = 0.20
FAST_TRACK_BASKET_MAX  = 5_000.0
FAST_TRACK_FRAUD_MAX   = 0.15

s3 = boto3.client("s3")


# ── data loaders ─────────────────────────────────────────────────────────────

def read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    logger.info(f"Reading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def try_read_s3_csv(bucket: str, key: str, fallback_cols: list[str]) -> pd.DataFrame:
    """Returns an empty DataFrame with fallback_cols if the object does not exist."""
    try:
        return read_s3_csv(bucket, key)
    except s3.exceptions.NoSuchKey:
        logger.warning(f"s3://{bucket}/{key} not found — defaulting to empty")
        return pd.DataFrame(columns=fallback_cols)
    except Exception as exc:
        logger.warning(f"Could not read s3://{bucket}/{key}: {exc} — defaulting to empty")
        return pd.DataFrame(columns=fallback_cols)


# ── feature builders ─────────────────────────────────────────────────────────

def build_customer_fraud_rates(hist_orders: pd.DataFrame, fraud_flags: pd.DataFrame) -> pd.DataFrame:
    """Per-customer share of orders with fraud_score > FRAUD_RATE_THRESH."""
    o2c   = hist_orders[["order_id", "customer_id"]]
    ff2   = fraud_flags[["order_id", "fraud_score"]].merge(o2c, on="order_id", how="left")
    total = o2c.groupby("customer_id").size().rename("total_orders")
    high  = (ff2[ff2["fraud_score"] > FRAUD_RATE_THRESH]
             .groupby("customer_id").size().rename("high_fraud_orders"))
    stats = total.to_frame().join(high, how="left").fillna(0)
    stats["fraud_rate"] = stats["high_fraud_orders"] / stats["total_orders"].clip(lower=1)
    return stats[["fraud_rate"]].reset_index()


def build_manual_approval_rates(actions: pd.DataFrame) -> pd.DataFrame:
    """
    Per-customer manual-review approval rate and count.
    'Approved' = action == 'closed_approved' for a manual-review decision.
    Returns columns: customer_id, manual_review_count, manual_approval_rate.
    """
    if actions.empty or "decision" not in actions.columns:
        return pd.DataFrame(columns=["customer_id", "manual_review_count", "manual_approval_rate"])

    manual = actions[actions["decision"] == "manual-review"].copy()
    if manual.empty:
        return pd.DataFrame(columns=["customer_id", "manual_review_count", "manual_approval_rate"])

    agg = manual.groupby("customer_id").agg(
        manual_review_count=("action", "count"),
        manual_approved_count=("action", lambda x: (x == "closed_approved").sum()),
    ).reset_index()
    agg["manual_approval_rate"] = agg["manual_approved_count"] / agg["manual_review_count"].clip(lower=1)
    return agg[["customer_id", "manual_review_count", "manual_approval_rate"]]


def build_approved_order_counts(actions: pd.DataFrame) -> pd.DataFrame:
    """Per-customer count of orders actioned as closed_approved (for new-customer graduation)."""
    if actions.empty or "action" not in actions.columns:
        return pd.DataFrame(columns=["customer_id", "approved_order_count"])
    approved = (actions[actions["action"] == "closed_approved"]
                .groupby("customer_id").size()
                .rename("approved_order_count")
                .reset_index())
    return approved


# ── scoring ───────────────────────────────────────────────────────────────────

def apply_decisions(df: pd.DataFrame, basket_p95: float, ltv_p95: float) -> pd.DataFrame:
    """Vectorised five-factor risk model with new-customer rules."""

    # ── 1. base risk (weights sum to 1) ───────────────────────────────────────
    fraud      = df["fraud_score"].fillna(0.0).clip(0, 1)
    basket_n   = df["basket_value"].fillna(0.0).clip(upper=basket_p95) / basket_p95
    ltv_n      = 1.0 - df["ltv"].fillna(0.0).clip(upper=ltv_p95) / ltv_p95
    base_risk  = (0.55 * fraud + 0.25 * basket_n + 0.20 * ltv_n).round(4)

    # ── 2. loyalty discount ───────────────────────────────────────────────────
    loyalty_d = (
        (df["tenure_days"].fillna(0) / 365) * (1 - df["fraud_rate"].fillna(0))
    ).clip(upper=LOYALTY_CAP).round(4)

    # ── 3. manual approval rate discount ─────────────────────────────────────
    eligible_mask = (
        (df["manual_review_count"].fillna(0) >= MANUAL_RATE_MIN_COUNT) &
        (df["manual_approval_rate"].fillna(0) > MANUAL_RATE_THRESHOLD)
    )
    manual_rate_d = pd.Series(0.0, index=df.index)
    manual_rate_d[eligible_mask] = (
        ((df.loc[eligible_mask, "manual_approval_rate"] - MANUAL_RATE_THRESHOLD)
         / (1.0 - MANUAL_RATE_THRESHOLD) * MANUAL_RATE_CAP)
        .clip(0, MANUAL_RATE_CAP)
    )
    manual_rate_d = manual_rate_d.round(4)

    # ── 4. raw risk before new-customer floor ─────────────────────────────────
    raw_risk = (base_risk - loyalty_d - manual_rate_d).clip(lower=0).round(4)

    # ── 5. new-customer flag and floor ────────────────────────────────────────
    is_new = (
        (df["tenure_days"].fillna(0)      < NEW_CUST_TENURE_DAYS) &
        (df["total_orders"].fillna(999)   < NEW_CUST_MAX_ORDERS) &
        (df["approved_order_count"].fillna(0) < NEW_CUST_MIN_APPROVED)
    )
    risk = raw_risk.copy()
    risk[is_new] = risk[is_new].clip(lower=NEW_CUST_RISK_FLOOR)

    # ── 6. decision labels ────────────────────────────────────────────────────
    is_fast_track = (
        is_new &
        (df["basket_value"].fillna(0) < FAST_TRACK_BASKET_MAX) &
        (fraud < FAST_TRACK_FRAUD_MAX)
    )

    decision = np.select(
        [
            # new customers can never auto-approve, but can decline
            is_new & (risk >= RISK_DECLINE),
            is_fast_track,
            is_new,                         # new + not fast-track + not decline
            risk < RISK_AUTO_APPROVE,
            risk >= RISK_DECLINE,
        ],
        [
            "decline",
            "manual-review",
            "manual-review",
            "auto-approve",
            "decline",
        ],
        default="manual-review",
    )

    # ── 7. decision reason ────────────────────────────────────────────────────
    new_flag    = np.where(is_new,        " | NEW_CUSTOMER_FLOOR",      "")
    fast_flag   = np.where(is_fast_track, " | fast-track-review",       "")
    manual_flag = np.where(
        eligible_mask,
        " | manual_rate_disc=" + manual_rate_d.astype(str)
        + "(rate=" + df["manual_approval_rate"].fillna(0).round(3).astype(str) + ")",
        "",
    )

    reason = (
        "risk_score="        + risk.astype(str)
        + " | base_risk="    + base_risk.astype(str)
        + " | loyalty_disc=" + loyalty_d.astype(str)
        + manual_flag
        + " | fraud_score="  + fraud.round(4).astype(str)
        + " | basket="       + df["basket_value"].fillna(0).round(2).astype(str)
        + " | ltv="          + df["ltv"].fillna(0).round(2).astype(str)
        + " | tenure_days="  + df["tenure_days"].fillna(0).astype(str)
        + " | fraud_rate="   + df["fraud_rate"].fillna(0).round(4).astype(str)
        + new_flag
        + fast_flag
    )

    out = df.copy()
    out["base_risk"]          = base_risk
    out["loyalty_discount"]   = loyalty_d
    out["manual_rate_discount"] = manual_rate_d
    out["risk_score"]         = risk
    out["is_new_customer"]    = is_new
    out["is_fast_track"]      = is_fast_track
    out["decision"]           = decision
    out["decision_reason"]    = reason
    return out


# ── handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    total_processed = 0

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]
        logger.info(f"Processing trigger: s3://{bucket}/{key}")

        try:
            # ── reference data ────────────────────────────────────────────────
            hist_orders = read_s3_csv(S3_BUCKET, ORDERS_HISTORY_KEY)
            customers   = read_s3_csv(S3_BUCKET, CUSTOMERS_KEY)[
                ["customer_id", "ltv", "region", "tenure_days", "total_orders"]
            ]
            fraud_flags = read_s3_csv(S3_BUCKET, FRAUD_FLAGS_KEY)
            actions     = try_read_s3_csv(
                S3_BUCKET, ACTIONS_HISTORY_KEY,
                fallback_cols=["customer_id", "decision", "action"],
            )

            # ── data-driven caps ──────────────────────────────────────────────
            basket_p95 = float(hist_orders["basket_value"].quantile(0.95))
            ltv_p95    = float(customers["ltv"].quantile(0.95))
            logger.info(f"Caps: basket_p95={basket_p95:,.0f}  ltv_p95={ltv_p95:,.0f}")

            # ── per-customer features ─────────────────────────────────────────
            cust_fraud_rates    = build_customer_fraud_rates(hist_orders, fraud_flags)
            cust_manual_rates   = build_manual_approval_rates(actions)
            cust_approved_counts = build_approved_order_counts(actions)

            customers = (
                customers
                .merge(cust_fraud_rates,     on="customer_id", how="left")
                .merge(cust_manual_rates,    on="customer_id", how="left")
                .merge(cust_approved_counts, on="customer_id", how="left")
            )
            customers["fraud_rate"]           = customers["fraud_rate"].fillna(0.0)
            customers["manual_review_count"]  = customers["manual_review_count"].fillna(0)
            customers["manual_approval_rate"] = customers["manual_approval_rate"].fillna(0.0)
            customers["approved_order_count"] = customers["approved_order_count"].fillna(0)

            # ── score incoming orders ─────────────────────────────────────────
            orders = read_s3_csv(bucket, key)
            logger.info(f"Loaded {len(orders):,} orders from {key}")

            df = (
                orders
                .merge(customers, on="customer_id", how="left")
                .merge(
                    fraud_flags[["order_id", "fraud_score", "flag_reason"]],
                    on="order_id", how="left",
                )
            )
            df["fraud_score"] = df["fraud_score"].fillna(0.0)

            df = apply_decisions(df, basket_p95, ltv_p95)

            ts = datetime.now(timezone.utc)
            df["basket_p95_used"] = round(basket_p95, 2)
            df["ltv_p95_used"]    = round(ltv_p95, 2)
            df["source_file"]     = key
            df["decided_at"]      = ts.isoformat()

            front = [
                "order_id", "customer_id", "decision", "decision_reason",
                "risk_score", "base_risk", "loyalty_discount", "manual_rate_discount",
                "fraud_score", "basket_value", "ltv",
                "tenure_days", "total_orders", "fraud_rate",
                "manual_review_count", "manual_approval_rate", "approved_order_count",
                "is_new_customer", "is_fast_track",
                "flag_reason", "payment_method", "payment_status", "fulfilment_status",
                "region", "basket_p95_used", "ltv_p95_used", "source_file", "decided_at",
            ]
            remaining = [c for c in df.columns if c not in front]
            df = df[front + remaining]

            stem    = key.rsplit("/", 1)[-1].replace(".csv", "")
            out_key = f"{DECISIONS_PREFIX}{stem}_{ts.strftime('%Y%m%dT%H%M%SZ')}.csv"

            buf = io.StringIO()
            df.to_csv(buf, index=False)
            s3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=buf.getvalue(), ContentType="text/csv")

            counts = df["decision"].value_counts().to_dict()
            logger.info(
                f"Wrote {len(df):,} decisions → s3://{S3_BUCKET}/{out_key} | "
                f"breakdown={counts} | "
                f"new_customers={df['is_new_customer'].sum()} | "
                f"fast_track={df['is_fast_track'].sum()} | "
                f"manual_rate_eligible={df[df['manual_rate_discount']>0].shape[0]}"
            )
            total_processed += len(df)

        except Exception:
            logger.exception(f"Fatal error processing s3://{bucket}/{key}")
            raise

    return {"statusCode": 200, "orders_processed": total_processed}
