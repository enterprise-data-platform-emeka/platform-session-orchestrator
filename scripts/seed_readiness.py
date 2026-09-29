"""Validate the current DMS full load, not merely the existence of old S3 files."""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import boto3

TABLES = {
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "seed_manifest",
    "seed_event_history",
}


def complete(stats, expected):
    tables = {x["TableName"]: x for x in stats if x["SchemaName"] == "public"}
    if not TABLES.issubset(tables):
        return False
    for name in TABLES:
        table = tables[name]
        if table["TableState"] != "Table completed" or table.get(
            "FullLoadErrorRows", 0
        ):
            return False
        if table.get("FullLoadRows", 0) < 1:
            return False
    return all(
        tables[name]["FullLoadRows"] == count for name, count in expected.items()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--environment", required=True, choices=["dev", "staging", "prod"]
    )
    parser.add_argument(
        "--profile", required=True, choices=["smoke", "customer-intelligence-36m"]
    )
    parser.add_argument("--stop-after-load", action="store_true")
    args = parser.parse_args()
    dms, s3 = boto3.client("dms"), boto3.client("s3")
    tasks = dms.describe_replication_tasks(
        Filters=[
            {
                "Name": "replication-task-id",
                "Values": [f"edp-{args.environment}-cdc-task"],
            }
        ]
    )["ReplicationTasks"]
    if len(tasks) != 1:
        raise RuntimeError("Expected one replication task")
    arn = tasks[0]["ReplicationTaskArn"]
    small = {
        "dev": (500, 200, 2000),
        "staging": (1000, 400, 5000),
        "prod": (2000, 800, 10000),
    }
    counts = (
        (20000, 1500, 300000)
        if args.profile == "customer-intelligence-36m"
        else small[args.environment]
    )
    expected = dict(zip(("customers", "products", "orders"), counts))
    expected["seed_manifest"] = 1
    start = datetime.fromisoformat(os.environ["DMS_LOAD_REQUESTED_AT"])
    for _ in range(120):
        task = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [arn]}]
        )["ReplicationTasks"][0]
        info = task.get("ReplicationTaskStats", {})
        if task["Status"] == "failed":
            raise RuntimeError("DMS task failed")
        stats = [
            x
            for page in dms.get_paginator("describe_table_statistics").paginate(
                ReplicationTaskArn=arn
            )
            for x in page["TableStatistics"]
        ]
        load_start = info.get("FullLoadStartDate")
        if (
            load_start
            and load_start >= start
            and info.get("FullLoadProgressPercent") == 100
            and info.get("TablesErrored", 0) == 0
            and complete(stats, expected)
        ):
            break
        time.sleep(15)
    else:
        raise RuntimeError(
            "Current seed full load did not complete with expected counts"
        )
    account = boto3.client("sts").get_caller_identity()["Account"]
    bucket = f"edp-{args.environment}-{account}-bronze"
    for table in TABLES:
        found = any(
            x["Key"].endswith(".parquet") and x["LastModified"] >= start
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=f"raw/public/{table}/"
            )
            for x in page.get("Contents", [])
        )
        if not found:
            raise RuntimeError(
                "Current load has no new Parquet data for a required table"
            )
    if args.stop_after_load:
        dms.stop_replication_task(ReplicationTaskArn=arn)
        for _ in range(120):
            status = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": [arn]}]
            )["ReplicationTasks"][0]["Status"]
            if status == "stopped":
                break
            time.sleep(5)
        else:
            raise RuntimeError("DMS task did not stop after seed load")
    marker = {
        "profile": args.profile,
        "required_tables": sorted(TABLES),
        "row_counts": expected,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "mode": "seed-only" if args.stop_after_load else "seed-and-live",
    }
    s3.put_object(
        Bucket=bucket,
        Key="metadata/seed-ready.json",
        Body=json.dumps(marker).encode(),
        ContentType="application/json",
    )
    print("Current seed full load and Bronze objects validated.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"Seed readiness failed ({type(exc).__name__}); downstream processing blocked."
        )
        raise SystemExit(1) from None
