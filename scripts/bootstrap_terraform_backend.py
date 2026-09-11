"""Create the S3 state bucket + DynamoDB lock table in the current AWS account.

Run with the Germany account credentials:

    aws sts get-caller-identity --profile dataforge-eu
    python scripts/bootstrap_terraform_backend.py --profile dataforge-eu
"""

from __future__ import annotations

import argparse
import json
import sys

import boto3
from botocore.exceptions import ClientError

REGION = "eu-central-1"
LOCK_TABLE = "dataforge-terraform-locks"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    sts = session.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    bucket = f"dataforge-terraform-state-{account_id}"

    print(f"Account: {account_id}")
    print(f"ARN:     {identity['Arn']}")
    print(f"Bucket:  {bucket}")
    print(f"Table:   {LOCK_TABLE}")

    s3 = session.client("s3")
    try:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print("Created state bucket.")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            print("State bucket already exists.")
        else:
            raise

    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        },
    )

    dynamodb = session.client("dynamodb")
    try:
        dynamodb.create_table(
            TableName=LOCK_TABLE,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "LockID", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "LockID", "KeyType": "HASH"}],
        )
        print("Created lock table.")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "ResourceInUseException":
            print("Lock table already exists.")
        else:
            raise

    backend = {
        "bucket": bucket,
        "key": "dataforge/terraform.tfstate",
        "region": REGION,
        "dynamodb_table": LOCK_TABLE,
        "encrypt": True,
    }
    print("\nWrite terraform/backends/eu.hcl with:")
    print(json.dumps(backend, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
