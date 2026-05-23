"""
Packages and deploys both Lambda functions, then wires up S3 triggers.

Run once:  python3 deploy.py
Re-deploy: python3 deploy.py   (idempotent — updates existing resources)
"""

import io
import json
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ── config ────────────────────────────────────────────────────────────────────

BUCKET     = "maxab-assessment-data"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# Derive region from the bucket so Lambdas land in the same region
# (S3 event notifications only invoke Lambdas in the same region as the bucket)
_loc = boto3.client("s3").get_bucket_location(Bucket=BUCKET)["LocationConstraint"]
REGION = _loc or "us-east-1"   # us-east-1 returns None from get_bucket_location
ROLE_NAME      = "maxab-lambda-role"
QUEUE_NAME     = "maxab-manual-review-queue"
RUNTIME        = "python3.12"
TIMEOUT        = 300   # seconds
MEMORY_MB      = 512

FUNCTIONS = {
    "maxab-order-decisioning": {
        "src":         Path("lambda/order_decisioning/lambda_function.py"),
        "handler":     "lambda_function.lambda_handler",
        "description": "Classifies orders as auto-approve / manual-review / decline",
        "env_extra":   {},
        "memory":      MEMORY_MB,
    },
    "maxab-decision-action": {
        "src":         Path("lambda/decision_action/lambda_function.py"),
        "handler":     "lambda_function.lambda_handler",
        "description": "Executes downstream action for each decision",
        "env_extra":   {},   # MANUAL_REVIEW_QUEUE_URL injected after queue creation
        "memory":      256,
    },
}

iam = boto3.client("iam",    region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sqs = boto3.client("sqs",    region_name=REGION)
s3  = boto3.client("s3",     region_name=REGION)


# ── IAM role ──────────────────────────────────────────────────────────────────

TRUST_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
})

INLINE_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{BUCKET}",
                f"arn:aws:s3:::{BUCKET}/*",
            ],
        },
        {
            "Sid": "SQSAccess",
            "Effect": "Allow",
            "Action": ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
            "Resource": f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{QUEUE_NAME}",
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ],
            "Resource": "arn:aws:logs:*:*:*",
        },
    ],
})


def ensure_role() -> str:
    try:
        role = iam.get_role(RoleName=ROLE_NAME)
        role_arn = role["Role"]["Arn"]
        print(f"  IAM role exists: {role_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=TRUST_POLICY,
            Description="Execution role for MaxAB Lambda pipeline",
        )
        role_arn = role["Role"]["Arn"]
        print(f"  Created IAM role: {role_arn}")
        time.sleep(10)   # propagation delay

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="maxab-lambda-policy",
        PolicyDocument=INLINE_POLICY,
    )
    return role_arn


# ── SQS queue ─────────────────────────────────────────────────────────────────

def ensure_queue() -> str:
    try:
        url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        print(f"  SQS queue exists: {url}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "AWS.SimpleQueueService.NonExistentQueue":
            raise
        url = sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={"MessageRetentionPeriod": "1209600"},  # 14 days
        )["QueueUrl"]
        print(f"  Created SQS queue: {url}")
    return url


# ── Lambda packaging ──────────────────────────────────────────────────────────

def build_zip(src: Path) -> bytes:
    """
    Returns a zip with the function code + pandas/numpy vendored inline.
    Uses a temp dir under /tmp to avoid polluting the project tree.
    """
    import tempfile, shutil
    with tempfile.TemporaryDirectory() as tmp:
        pkg = Path(tmp) / "pkg"
        pkg.mkdir()
        print(f"    pip install pandas numpy → {pkg}")
        subprocess.run(
            [sys.executable, "-m", "pip", "install",
             "pandas", "numpy", "--target", str(pkg), "--quiet",
             "--python-version", "3.12", "--only-binary=:all:"],
            check=True,
        )
        # Copy function source
        shutil.copy(src, pkg / "lambda_function.py")

        # Zip everything
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in pkg.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(pkg))
        size_mb = buf.tell() / 1_048_576
        print(f"    zip size: {size_mb:.1f} MB")
        return buf.getvalue()


# ── Lambda deploy ─────────────────────────────────────────────────────────────

def deploy_function(name: str, cfg: dict, role_arn: str, env_vars: dict) -> str:
    code = build_zip(cfg["src"])
    env  = {"Variables": {"S3_BUCKET": BUCKET, **cfg["env_extra"], **env_vars}}

    try:
        lam.get_function(FunctionName=name)
        print(f"    Updating function code …")
        lam.update_function_code(FunctionName=name, ZipFile=code)
        # Wait for update to propagate before updating config
        waiter = lam.get_waiter("function_updated_v2")
        waiter.wait(FunctionName=name)
        lam.update_function_configuration(
            FunctionName=name,
            Handler=cfg["handler"],
            Timeout=TIMEOUT,
            MemorySize=cfg["memory"],
            Environment=env,
        )
        fn_arn = lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]
        print(f"    Updated: {fn_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        print(f"    Creating function …")
        resp = lam.create_function(
            FunctionName=name,
            Runtime=RUNTIME,
            Role=role_arn,
            Handler=cfg["handler"],
            Code={"ZipFile": code},
            Description=cfg["description"],
            Timeout=TIMEOUT,
            MemorySize=cfg["memory"],
            Environment=env,
        )
        fn_arn = resp["FunctionArn"]
        waiter = lam.get_waiter("function_active_v2")
        waiter.wait(FunctionName=name)
        print(f"    Created: {fn_arn}")
    return fn_arn


def add_s3_permission(fn_name: str, stmt_id: str) -> None:
    """Grant S3 permission to invoke the function (idempotent)."""
    try:
        lam.remove_permission(FunctionName=fn_name, StatementId=stmt_id)
    except ClientError:
        pass
    lam.add_permission(
        FunctionName=fn_name,
        StatementId=stmt_id,
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{BUCKET}",
        SourceAccount=ACCOUNT_ID,
    )


# ── S3 event notifications ────────────────────────────────────────────────────

def configure_s3_triggers(decisioning_arn: str, action_arn: str) -> None:
    """
    Both notifications must be set in a single put_bucket_notification_configuration
    call — a second call would overwrite the first.
    """
    config = {
        "LambdaFunctionConfigurations": [
            {
                "Id":                  "TriggerOrderDecisioning",
                "LambdaFunctionArn":   decisioning_arn,
                "Events":              ["s3:ObjectCreated:*"],
                "Filter": {"Key": {"FilterRules": [
                    {"Name": "prefix", "Value": "raw/orders/"},
                    {"Name": "suffix", "Value": ".csv"},
                ]}},
            },
            {
                "Id":                  "TriggerDecisionAction",
                "LambdaFunctionArn":   action_arn,
                "Events":              ["s3:ObjectCreated:*"],
                "Filter": {"Key": {"FilterRules": [
                    {"Name": "prefix", "Value": "decisions/"},
                    {"Name": "suffix", "Value": ".csv"},
                ]}},
            },
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration=config,
    )
    print(f"  S3 triggers configured on bucket {BUCKET}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'─'*60}")
    print(f"  Deploying MaxAB Lambda pipeline")
    print(f"  Account: {ACCOUNT_ID}  |  Region: {REGION}  |  Bucket: {BUCKET}")
    print(f"{'─'*60}\n")

    print("1/4  IAM role …")
    role_arn = ensure_role()

    print("\n2/4  SQS queue …")
    queue_url = ensure_queue()

    print("\n3/4  Lambda functions …")
    print(f"  → maxab-order-decisioning")
    decisioning_arn = deploy_function(
        "maxab-order-decisioning",
        FUNCTIONS["maxab-order-decisioning"],
        role_arn,
        env_vars={},
    )
    add_s3_permission("maxab-order-decisioning", "AllowS3InvokeOrderDecisioning")

    print(f"  → maxab-decision-action")
    action_arn = deploy_function(
        "maxab-decision-action",
        FUNCTIONS["maxab-decision-action"],
        role_arn,
        env_vars={"MANUAL_REVIEW_QUEUE_URL": queue_url},
    )
    add_s3_permission("maxab-decision-action", "AllowS3InvokeDecisionAction")

    print("\n4/4  S3 event notifications …")
    configure_s3_triggers(decisioning_arn, action_arn)

    print(f"\n{'─'*60}")
    print("  Deployment complete.\n")
    print(f"  Order decisioning:  {decisioning_arn}")
    print(f"  Decision action:    {action_arn}")
    print(f"  Manual review queue:{queue_url}")
    print(f"\n  To test: re-upload orders.csv to trigger the pipeline:")
    print(f"  aws s3 cp orders.csv s3://{BUCKET}/raw/orders/orders_test.csv")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    main()
