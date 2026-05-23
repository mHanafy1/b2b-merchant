# MaxAB B2B Order Decisioning Pipeline

Event-driven AWS pipeline that scores incoming B2B wholesale orders with a five-factor risk model and routes each order to the appropriate downstream action — all triggered automatically by S3 file uploads with no polling or orchestration layer.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  S3  maxab-assessment-data  (eu-north-1)                            │
│                                                                     │
│  raw/orders/*.csv                                                   │
│        │  ObjectCreated event                                       │
│        ▼                                                            │
│  ┌─────────────────────────────────────────────────────┐           │
│  │  Lambda 1 — maxab-order-decisioning  (1 GB, 300 s)  │           │
│  │                                                     │           │
│  │  Reads: orders, customers, fraud_flags, actions     │           │
│  │                                                     │           │
│  │  Five-factor risk model                             │           │
│  │    base_risk   = 0.55×fraud_score                   │           │
│  │               + 0.25×min(basket/p95, 1)             │           │
│  │               + 0.20×(1 − min(ltv/p95, 1))         │           │
│  │    − loyalty_discount   (up to 0.15, tenure-based)  │           │
│  │    − manual_rate_disc   (up to 0.10, if ≥5 reviews) │           │
│  │                                                     │           │
│  │  Decisions: auto-approve / manual-review / decline  │           │
│  │  New-customer rules: risk floor 0.20, fast-track    │           │
│  │                                                     │           │
│  │  Writes → decisions/*.csv                           │           │
│  └─────────────────────────────────────────────────────┘           │
│        │  ObjectCreated event                                       │
│        ▼                                                            │
│  ┌─────────────────────────────────────────────────────┐           │
│  │  Lambda 2 — maxab-decision-action  (1 GB, 300 s)    │           │
│  │                                                     │           │
│  │  auto-approve  → closed_approved                    │           │
│  │  manual-review → escalated_to_review + SQS msg      │           │
│  │  decline       → payment_retry (next available rail)│           │
│  │                                                     │           │
│  │  Writes → actions/*.csv                             │           │
│  └─────────────────────────────────────────────────────┘           │
│                                          │                          │
└──────────────────────────────────────────┼──────────────────────────┘
                                           ▼
                              SQS  maxab-manual-review-queue
                              (14-day retention, consumed by
                               ops team / review application)
```

### S3 Prefix Layout

| Prefix | Purpose |
|---|---|
| `raw/orders/` | Input order CSVs — uploading here starts the pipeline |
| `raw/customers/` | Customer master: tenure, LTV, total_orders |
| `raw/fraud_flags/` | Per-order fraud scores and flag reasons |
| `raw/actions/` | Historical actions reference (closed_approved history) |
| `decisions/` | Lambda 1 output — one CSV per input file |
| `actions/` | Lambda 2 output — one CSV per decisions file |

### Decision Logic Summary

| Condition | Decision |
|---|---|
| risk ≥ 0.55 | decline |
| 0.35 ≤ risk < 0.55 | manual-review |
| risk < 0.35 | auto-approve |
| New customer (tenure < 30d, orders < 3, approved < 3) | minimum: manual-review; fast-track if basket < 5k & fraud < 0.15 |

---

## Cost Envelope

All estimates assume **eu-north-1 (Stockholm)** pricing. The pipeline is event-driven — each upload to `raw/orders/` triggers exactly one run, so cost scales linearly with the number of orders in each file. In normal operation you upload only that day's new orders, not the full history.

| Service | Usage per run (1,000 orders) | Estimated cost |
|---|---|---|
| Lambda 1 | ~0.1 s × 1 GB | ~$0.000002 |
| Lambda 2 | ~0.4 s × 1 GB | ~$0.000006 |
| S3 storage | ~0.6 MB output/run | negligible |
| S3 PUT/GET requests | ~20 requests/run | < $0.0001 |
| SQS | ~70 messages/run (~7% manual-review) | < $0.0001 |
| CloudWatch Logs | ~0.01 MB/run | negligible |

**Rough total: < $0.0001 per 1,000 orders processed.** At 10,000 orders/day × 30 days the monthly bill is under $0.10, dominated by SQS.

> The 200,000-order one-shot test run (full historical dataset) cost ~$0.02 total and is not representative of steady-state production cost.

---

## Deploy from Scratch

### Prerequisites

- AWS CLI configured (`aws configure`) with permissions to create IAM, Lambda, S3, SQS, and CloudWatch resources
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) installed
- Python 3.12

### 1. Build Linux-compatible dependency packages

The Lambda functions use numpy and pandas, which must be compiled for Linux (Amazon Linux 2023 / `manylinux2014_x86_64`). Run this once before deploying:

```bash
# Lambda 1
pip install \
  --platform manylinux2014_x86_64 \
  --target lambda/order_decisioning/vendor \
  --only-binary=:all: \
  --python-version 3.12 \
  --implementation cp \
  numpy pandas

# Lambda 2
pip install \
  --platform manylinux2014_x86_64 \
  --target lambda/decision_action/vendor \
  --only-binary=:all: \
  --python-version 3.12 \
  --implementation cp \
  numpy pandas
```

Then add a `sys.path` insert at the top of each `lambda_function.py` if vendoring inline, **or** update the `CodeUri` in `template.yaml` to point to a pre-packaged ZIP. The simplest approach is the SAM build step below, which handles packaging automatically when a `requirements.txt` is present.

Alternatively, create `lambda/order_decisioning/requirements.txt` and `lambda/decision_action/requirements.txt` each containing:

```
numpy
pandas
```

Then SAM build will install them with the correct platform automatically.

### 2. SAM build and deploy

```bash
# Build (resolves dependencies per requirements.txt)
sam build

# Deploy (interactive first time — saves config to samconfig.toml)
sam deploy --guided \
  --stack-name maxab-pipeline \
  --region eu-north-1 \
  --capabilities CAPABILITY_NAMED_IAM

# Accept all defaults, or override the bucket name:
#   Parameter BucketName [maxab-assessment-data]:
```

SAM will create all resources in the correct dependency order and wire the S3 → Lambda event notifications automatically.

### 3. Upload reference data

After the stack is created, upload the seed data files:

```bash
BUCKET=maxab-assessment-data

aws s3 cp customers.csv       s3://$BUCKET/raw/customers/customers.csv       --region eu-north-1
aws s3 cp fraud_flags.csv     s3://$BUCKET/raw/fraud_flags/fraud_flags.csv   --region eu-north-1
aws s3 cp order_items.csv     s3://$BUCKET/raw/order_items/order_items.csv   --region eu-north-1
aws s3 cp actions_reference.csv s3://$BUCKET/raw/actions/actions_reference.csv --region eu-north-1
```

### 4. Trigger the pipeline

Upload any orders CSV to `raw/orders/`:

```bash
aws s3 cp orders.csv s3://$BUCKET/raw/orders/orders.csv --region eu-north-1
```

Lambda 1 fires within seconds, writes to `decisions/`, which triggers Lambda 2, which writes to `actions/` and sends SQS messages for manual-review orders.

### 5. Monitor

```bash
# Lambda 1 logs (tail)
aws logs tail /aws/lambda/maxab-order-decisioning --follow --region eu-north-1

# Lambda 2 logs (tail)
aws logs tail /aws/lambda/maxab-decision-action --follow --region eu-north-1

# Check actions output
aws s3 ls s3://$BUCKET/actions/ --region eu-north-1
```

---

## Tear Down

### Option A — SAM delete (recommended)

Removes the CloudFormation stack and all resources it manages:

```bash
sam delete --stack-name maxab-pipeline --region eu-north-1
```

SAM will prompt before deleting. The S3 bucket must be empty first — CloudFormation cannot delete a non-empty bucket.

```bash
# Empty the bucket first
aws s3 rm s3://maxab-assessment-data --recursive --region eu-north-1

# Then delete the stack
sam delete --stack-name maxab-pipeline --region eu-north-1
```

### Option B — Manual AWS CLI teardown

Use this if the stack was deployed without SAM, or if the CloudFormation stack is in a broken state:

```bash
REGION=eu-north-1
BUCKET=maxab-assessment-data

# 1. Empty and delete the S3 bucket
aws s3 rm s3://$BUCKET --recursive --region $REGION
aws s3api delete-bucket --bucket $BUCKET --region $REGION

# 2. Delete Lambda functions
aws lambda delete-function --function-name maxab-order-decisioning --region $REGION
aws lambda delete-function --function-name maxab-decision-action   --region $REGION

# 3. Delete SQS queue
aws sqs delete-queue \
  --queue-url "https://sqs.$REGION.amazonaws.com/$(aws sts get-caller-identity --query Account --output text)/maxab-manual-review-queue" \
  --region $REGION

# 4. Delete IAM role and its inline policy
aws iam delete-role-policy --role-name maxab-lambda-role --policy-name maxab-lambda-policy
aws iam delete-role        --role-name maxab-lambda-role

# 5. Delete CloudWatch log groups
aws logs delete-log-group --log-group-name /aws/lambda/maxab-order-decisioning --region $REGION
aws logs delete-log-group --log-group-name /aws/lambda/maxab-decision-action   --region $REGION
```

---

## Repository Layout

```
.
├── template.yaml                        # AWS SAM / CloudFormation template
├── deploy.py                            # Legacy deploy script (pre-SAM)
├── generate_and_upload.py               # Synthetic dataset generator
├── athena_setup.sql                     # Athena DDL for querying S3 data
├── orders.csv                           # 200k-order dataset
├── customers.csv                        # Customer master
├── fraud_flags.csv                      # Per-order fraud scores
├── order_items.csv                      # Line-item detail
├── actions_reference.csv                # Historical actions (seed data)
└── lambda/
    ├── order_decisioning/
    │   └── lambda_function.py           # Lambda 1 — five-factor risk model
    └── decision_action/
        └── lambda_function.py           # Lambda 2 — action router
```
