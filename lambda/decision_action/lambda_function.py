"""
Lambda 2 — Decision Action
Trigger: s3:ObjectCreated:* on prefix decisions/

Reads a decisions file written by Lambda 1 and takes a downstream action
per order based on the decision label:

  auto-approve  → closed_approved   (no further processing needed)
  manual-review → escalated_to_review  + SQS message to review queue
  decline       → payment_retry     (rotate to the next available payment rail)

Writes a full action record with lineage to s3://…/actions/.

Environment variables:
  S3_BUCKET              – target bucket (default: maxab-assessment-data)
  MANUAL_REVIEW_QUEUE_URL – SQS queue URL for manual-review escalations
"""

import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET               = os.environ.get("S3_BUCKET", "maxab-assessment-data")
MANUAL_REVIEW_QUEUE_URL = os.environ.get("MANUAL_REVIEW_QUEUE_URL", "")
ACTIONS_PREFIX          = "actions/"

# Ordered preference list for payment retry (first rail different from original wins)
PAYMENT_RAILS = ["bank_transfer", "mobile_wallet", "credit_card", "BNPL", "cash"]

s3  = boto3.client("s3")
sqs = boto3.client("sqs")


# ── helpers ───────────────────────────────────────────────────────────────────

def read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    logger.info(f"Reading s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def next_payment_rail(current: str) -> str:
    for rail in PAYMENT_RAILS:
        if rail != current:
            return rail
    return "bank_transfer"


def escalate_to_sqs(order_id: str, row: pd.Series) -> None:
    if not MANUAL_REVIEW_QUEUE_URL:
        logger.warning(f"MANUAL_REVIEW_QUEUE_URL not set — skipping SQS for order {order_id}")
        return
    payload = {
        "order_id":        order_id,
        "customer_id":     row.get("customer_id"),
        "basket_value":    row.get("basket_value"),
        "risk_score":      row.get("risk_score"),
        "fraud_score":     row.get("fraud_score"),
        "flag_reason":     row.get("flag_reason"),
        "decision_reason": row.get("decision_reason"),
        "escalated_at":    datetime.now(timezone.utc).isoformat(),
    }
    sqs.send_message(
        QueueUrl=MANUAL_REVIEW_QUEUE_URL,
        MessageBody=json.dumps(payload, default=str),
        MessageAttributes={
            "OrderId": {"StringValue": str(order_id), "DataType": "String"},
        },
    )
    logger.info(f"Escalated order {order_id} to SQS manual review queue")


def build_action_df(decisions: pd.DataFrame, source_key: str) -> pd.DataFrame:
    """Vectorised action assignment with per-row SQS side-effect for escalations."""
    ts = datetime.now(timezone.utc).isoformat()

    action = np.select(
        [decisions["decision"] == "auto-approve",
         decisions["decision"] == "manual-review",
         decisions["decision"] == "decline"],
        ["closed_approved",
         "escalated_to_review",
         "payment_retry"],
        default="unknown",
    )

    retry_rail = decisions["payment_method"].apply(
        lambda m: next_payment_rail(str(m)) if pd.notna(m) else "bank_transfer"
    )

    action_detail = np.select(
        [decisions["decision"] == "auto-approve",
         decisions["decision"] == "manual-review",
         decisions["decision"] == "decline"],
        [
            "Order closed — no further action required",
            "Queued for human review: " + decisions["decision_reason"].fillna("").astype(str),
            "Retrying on " + retry_rail + " (original: " + decisions["payment_method"].fillna("unknown").astype(str) + "): " + decisions["decision_reason"].fillna("").astype(str),
        ],
        default="Unrecognised decision value",
    )

    # Fire SQS for every manual-review row
    manual_mask = decisions["decision"] == "manual-review"
    if manual_mask.any():
        logger.info(f"Escalating {manual_mask.sum():,} orders to manual review queue")
        for _, row in decisions[manual_mask].iterrows():
            try:
                escalate_to_sqs(row["order_id"], row)
            except Exception:
                logger.exception(f"SQS escalation failed for order {row['order_id']} — continuing")

    result = pd.DataFrame({
        "order_id":              decisions["order_id"],
        "customer_id":           decisions.get("customer_id", pd.Series(dtype=str)),
        "decision":              decisions["decision"],
        "action":                action,
        "action_detail":         action_detail,
        "risk_score":            decisions.get("risk_score",      pd.Series(dtype=float)),
        "fraud_score":           decisions.get("fraud_score",     pd.Series(dtype=float)),
        "basket_value":          decisions.get("basket_value",    pd.Series(dtype=float)),
        "ltv":                   decisions.get("ltv",             pd.Series(dtype=float)),
        "payment_method":        decisions.get("payment_method",  pd.Series(dtype=str)),
        "retry_rail":            np.where(decisions["decision"] == "decline", retry_rail, pd.NA),
        "decision_reason":       decisions.get("decision_reason", pd.Series(dtype=str)),
        "source_decision_file":  source_key,
        "actioned_at":           ts,
    })
    return result


# ── handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    total_processed = 0

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]
        logger.info(f"Processing trigger: s3://{bucket}/{key}")

        try:
            decisions = read_s3_csv(bucket, key)
            logger.info(f"Loaded {len(decisions):,} decisions")

            actions = build_action_df(decisions, key)

            ts      = datetime.now(timezone.utc)
            stem    = key.rsplit("/", 1)[-1].replace(".csv", "")
            out_key = f"{ACTIONS_PREFIX}{stem}_{ts.strftime('%Y%m%dT%H%M%SZ')}.csv"

            buf = io.StringIO()
            actions.to_csv(buf, index=False)
            s3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=buf.getvalue(), ContentType="text/csv")

            counts = actions["action"].value_counts().to_dict()
            logger.info(f"Wrote {len(actions):,} actions → s3://{S3_BUCKET}/{out_key} | breakdown={counts}")
            total_processed += len(actions)

        except Exception:
            logger.exception(f"Fatal error processing s3://{bucket}/{key}")
            raise

    return {"statusCode": 200, "decisions_processed": total_processed}
