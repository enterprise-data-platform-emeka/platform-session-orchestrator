"""Reject reuse after daily purge, or when the chosen profile differs."""

import json
import os

import boto3

from seed_readiness import TABLES

if __name__ == "__main__":
    try:
        s3 = boto3.client("s3")
        bucket = f"edp-{os.environ['ENV']}-{os.environ['ACCOUNT_ID']}-bronze"
        marker = json.loads(
            s3.get_object(Bucket=bucket, Key="metadata/seed-ready.json")["Body"].read()
        )
        if marker["profile"] != os.environ["SEED_PROFILE"]:
            raise ValueError("Profile mismatch")
        for table in TABLES:
            if not any(
                x["Key"].endswith(".parquet")
                for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=f"raw/public/{table}/"
                )
                for x in page.get("Contents", [])
            ):
                raise ValueError("Missing table")
        print("Retained seed data is available.")
    except Exception:
        print(
            "Retained dataset is absent, incomplete, or incompatible. Start with seed-only."
        )
        raise SystemExit(1) from None
