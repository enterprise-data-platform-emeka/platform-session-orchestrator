"""Fail-closed cleanup of explicit session buckets, never Terraform backend state.

Run only after runtime teardown succeeded. Names come from the authenticated
account and a validated environment; no arbitrary bucket argument is accepted.
"""

import argparse
import os
import time

import boto3
from botocore.exceptions import ClientError

SUFFIXES = (
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "athena-results",
    "glue-scripts",
    "mwaa-dags",
)
TABLES = (
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
)


def bucket_names(environment, account):
    if (
        environment not in ("dev", "staging", "prod")
        or len(account) != 12
        or not account.isdigit()
    ):
        raise ValueError("Invalid environment or account")
    return [f"edp-{environment}-{account}-{suffix}" for suffix in SUFFIXES]


def empty_bucket(s3, bucket, account):
    owner = dict(Bucket=bucket, ExpectedBucketOwner=account)
    try:
        s3.head_bucket(**owner)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return
        raise
    # Re-read the first page after each delete. Pagination over a mutating
    # version list can skip keys; every version ID must be explicitly deleted.
    for _ in range(100000):
        page = s3.list_object_versions(**owner, MaxKeys=1000)
        objects = [
            {"Key": x["Key"], "VersionId": x["VersionId"]}
            for x in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if not objects:
            break
        result = s3.delete_objects(**owner, Delete={"Objects": objects, "Quiet": True})
        if result.get("Errors"):
            raise RuntimeError("S3 rejected one or more version deletions")
    else:
        raise RuntimeError("Version cleanup exceeded its bound")
    # Also handles unversioned keys and suspended-versioning null versions.
    for _ in range(100000):
        objects = [
            {"Key": x["Key"]}
            for x in s3.list_objects_v2(**owner, MaxKeys=1000).get("Contents", [])
        ]
        if not objects:
            break
        if s3.delete_objects(**owner, Delete={"Objects": objects, "Quiet": True}).get(
            "Errors"
        ):
            raise RuntimeError("S3 rejected one or more object deletions")
    else:
        raise RuntimeError("Object cleanup exceeded its bound")
    for _ in range(100000):
        uploads = s3.list_multipart_uploads(**owner, MaxUploads=1000).get("Uploads", [])
        if not uploads:
            break
        for upload in uploads:
            s3.abort_multipart_upload(
                **owner, Key=upload["Key"], UploadId=upload["UploadId"]
            )
    else:
        raise RuntimeError("Multipart cleanup exceeded its bound")
    remaining = s3.list_object_versions(**owner, MaxKeys=1)
    if (
        remaining.get("Versions")
        or remaining.get("DeleteMarkers")
        or s3.list_objects_v2(**owner, MaxKeys=1).get("Contents")
        or s3.list_multipart_uploads(**owner, MaxUploads=1).get("Uploads")
    ):
        raise RuntimeError("Bucket is not empty after cleanup")


def stop_glue(glue, environment):
    """Silver jobs are deployed outside Terraform and survive teardown."""
    for table in TABLES:
        name = f"edp-{environment}-{table}"
        for _ in range(120):
            try:
                runs = [
                    r
                    for page in glue.get_paginator("get_job_runs").paginate(
                        JobName=name
                    )
                    for r in page["JobRuns"]
                    if r["JobRunState"]
                    in ("STARTING", "RUNNING", "STOPPING", "WAITING")
                ]
            except glue.exceptions.EntityNotFoundException:
                break
            if not runs:
                break
            for offset in range(0, len(runs), 25):
                result = glue.batch_stop_job_run(
                    JobName=name,
                    JobRunIds=[r["Id"] for r in runs[offset : offset + 25]],
                )
                if result.get("Errors"):
                    raise RuntimeError("Could not stop all Glue runs")
            time.sleep(5)
        else:
            raise RuntimeError("Glue runs did not stop")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["stop-glue", "empty"])
    parser.add_argument(
        "--environment", required=True, choices=["dev", "staging", "prod"]
    )
    args = parser.parse_args()
    account = boto3.client("sts").get_caller_identity()["Account"]
    buckets = bucket_names(args.environment, account)
    if account != os.environ.get("AWS_ACCOUNT_ID"):
        raise RuntimeError(
            "Authenticated account does not match the selected environment"
        )
    stop_glue(boto3.client("glue"), args.environment)
    if args.action == "empty":
        s3 = boto3.client("s3")
        for bucket in buckets:
            empty_bucket(s3, bucket, account)
        print(
            "Session buckets verified empty, including versions and multipart uploads."
        )
        print("Terraform backend storage is excluded.")
    else:
        print("Standalone Glue writers stopped.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Do not print AWS payloads/identifiers in public workflow logs.
        print(
            f"Session data cleanup failed ({type(exc).__name__}); cleanup is incomplete."
        )
        raise SystemExit(1) from None
