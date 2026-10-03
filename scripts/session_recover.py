"""Targeted recovery on existing infrastructure. No Terraform or full reseed.

Public output is deliberately neutral. AWS errors stay out of public CI logs.
The seed repair journal lives in versioned Bronze and is removed by daily destroy.
"""

import argparse
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

TABLES = (
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
)
RELOAD = ("payments", "seed_event_history", "seed_manifest")
JOURNAL = "metadata/recovery/payment-method-v2.json"
ACTIVE = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def now():
    return datetime.now(timezone.utc)


def wait_for(check, label, attempts=120, interval=10):
    for _ in range(attempts):
        result = check()
        if result:
            return result
        time.sleep(interval)
    raise RuntimeError(f"{label} timed out; inspect AWS before retrying")


def reload_complete(stats, counts, requested):
    tables = {x["TableName"]: x for x in stats if x["SchemaName"] == "public"}
    return all(
        name in tables
        and tables[name]["TableState"] == "Table completed"
        and tables[name].get(
            "FullLoadStartTime", datetime.min.replace(tzinfo=timezone.utc)
        )
        >= requested
        and tables[name].get("FullLoadRows") == counts[name]
        and tables[name].get("FullLoadErrorRows", 0) == 0
        for name in RELOAD
    )


class Recovery:
    def __init__(self, env, account, clients=None):
        require(env in ("dev", "staging", "prod"), "Invalid environment")
        require(len(account) == 12 and account.isdigit(), "Invalid account")
        self.env, self.account = env, account
        self.prefix = f"edp-{env}"
        self.bucket = f"{self.prefix}-{account}-bronze"
        self.owner = dict(Bucket=self.bucket, ExpectedBucketOwner=account)
        self.clients = clients or {
            name: boto3.client(name)
            for name in (
                "sts",
                "s3",
                "ecs",
                "ec2",
                "rds",
                "dms",
                "glue",
                "stepfunctions",
                "mwaa",
                "logs",
            )
        }
        for name, client in self.clients.items():
            setattr(self, name, client)

    def read_json(self, key, optional=False):
        try:
            return json.loads(self.s3.get_object(**self.owner, Key=key)["Body"].read())
        except ClientError as exc:
            if optional and exc.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def write_json(self, key, value):
        self.s3.put_object(
            **self.owner,
            Key=key,
            Body=json.dumps(value).encode(),
            ContentType="application/json",
        )

    def checkpoint(self, journal, stage):
        journal.update(stage=stage, checked_at=now().isoformat())
        self.write_json(JOURNAL, journal)
        print(f"Recovery checkpoint: {stage}.", flush=True)

    def preflight(self):
        require(
            self.sts.get_caller_identity()["Account"] == self.account,
            "Authenticated account mismatch",
        )
        self.s3.head_bucket(**self.owner)
        require(
            self.s3.get_bucket_versioning(**self.owner).get("Status") == "Enabled",
            "Bronze versioning is required",
        )
        # Recovery currently supports the Step Functions session path. Do not race Airflow.
        try:
            self.mwaa.get_environment(Name=f"{self.prefix}-mwaa")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        else:
            raise RuntimeError(
                "An MWAA environment exists; recovery requires an idle Step Functions session"
            )
        arn = f'arn:aws:states:{os.environ["AWS_REGION"]}:{self.account}:stateMachine:{self.prefix}-pipeline'
        require(
            not self.stepfunctions.list_executions(
                stateMachineArn=arn, statusFilter="RUNNING", maxResults=1
            )["executions"],
            "Pipeline execution is active; wait for it to finish",
        )
        for table in (*TABLES, "run-dbt"):
            runs = self.glue.get_paginator("get_job_runs").paginate(
                JobName=f"{self.prefix}-{table}"
            )
            require(
                not any(
                    r["JobRunState"] in ACTIVE for page in runs for r in page["JobRuns"]
                ),
                "A Glue job is active; wait for it to finish",
            )
        crawler = self.glue.get_crawler(Name=f"{self.prefix}-silver-crawler")["Crawler"]
        require(crawler["State"] == "READY", "Silver crawler is active")
        for desired in ("RUNNING", "PENDING"):
            pages = self.ecs.get_paginator("list_tasks").paginate(
                cluster=f"{self.prefix}-cdc-simulator", desiredStatus=desired
            )
            require(
                not any(p["taskArns"] for p in pages),
                "A source task is active; wait for it to finish",
            )
        marker = self.read_json("metadata/seed-ready.json")
        require(
            marker.get("mode") == "seed-only",
            "Recovery requires a validated seed-only session",
        )
        task = self.dms_task()
        require(
            task["Status"] in ("stopped", "running"),
            "DMS is not in a recoverable idle/running state",
        )
        endpoint = self.dms.describe_endpoints(
            Filters=[{"Name": "endpoint-arn", "Values": [task["TargetEndpointArn"]]}]
        )["Endpoints"]
        require(len(endpoint) == 1, "Expected one DMS target endpoint")
        target = endpoint[0].get("S3Settings", {})
        require(
            target.get("BucketName") == self.bucket
            and target.get("BucketFolder", "").strip("/") == "raw"
            and target.get("DataFormat") == "parquet"
            and task.get("MigrationType") == "full-load-and-cdc",
            "DMS target differs from the supported seed layout",
        )
        return marker

    def dms_task(self):
        tasks = self.dms.describe_replication_tasks(
            Filters=[
                {"Name": "replication-task-id", "Values": [f"{self.prefix}-cdc-task"]}
            ]
        )["ReplicationTasks"]
        require(len(tasks) == 1, "Expected one existing DMS task")
        return tasks[0]

    def stop_dms(self):
        task = self.dms_task()
        if task["Status"] == "running":
            self.dms.stop_replication_task(
                ReplicationTaskArn=task["ReplicationTaskArn"]
            )
        elif task["Status"] != "stopped":
            require(task["Status"] == "stopping", "DMS cannot be safely stopped")
        wait_for(lambda: self.dms_task()["Status"] == "stopped", "DMS stop")

    def source_repair(self, image, journal):
        family = f"{self.prefix}-cdc-simulator"
        definition = self.ecs.describe_task_definition(taskDefinition=family)[
            "taskDefinition"
        ]
        options = None
        for container in definition["containerDefinitions"]:
            if container["name"] == "simulator":
                container["image"] = image
                options = container["logConfiguration"]["options"]
        require(options is not None, "Simulator container is absent")
        for key in (
            "taskDefinitionArn",
            "revision",
            "status",
            "requiresAttributes",
            "compatibilities",
            "registeredAt",
            "registeredBy",
            "deregisteredAt",
        ):
            definition.pop(key, None)
        registered = self.ecs.register_task_definition(**definition)["taskDefinition"][
            "taskDefinitionArn"
        ]
        database = self.rds.describe_db_instances(
            DBInstanceIdentifier=f"{self.prefix}-source-db"
        )["DBInstances"][0]
        require(
            database["DBInstanceStatus"] == "available",
            "Source database is not available",
        )
        vpc = database["DBSubnetGroup"]["VpcId"]
        groups = self.ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [f"{family}-sg"]},
                {"Name": "vpc-id", "Values": [vpc]},
            ]
        )["SecurityGroups"]
        require(len(groups) == 1, "Expected one simulator security group")
        result = self.ecs.run_task(
            cluster=family,
            taskDefinition=registered,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [
                        x["SubnetIdentifier"]
                        for x in database["DBSubnetGroup"]["Subnets"]
                    ],
                    "securityGroups": [groups[0]["GroupId"]],
                    "assignPublicIp": "DISABLED",
                }
            },
            overrides={
                "containerOverrides": [
                    {"name": "simulator", "command": ["repair-payment-method"]}
                ]
            },
        )
        require(
            not result.get("failures") and len(result["tasks"]) == 1,
            "Source repair task could not start",
        )
        task_arn = result["tasks"][0]["taskArn"]
        journal["task_arn"] = task_arn  # Private checkpoint, never printed.
        self.checkpoint(journal, "source-repairing")

        def done():
            response = self.ecs.describe_tasks(cluster=family, tasks=[task_arn])
            require(
                not response.get("failures") and len(response["tasks"]) == 1,
                "Source task cannot be inspected",
            )
            task = response["tasks"][0]
            if task["lastStatus"] != "STOPPED":
                return False
            containers = task.get("containers", [])
            require(
                containers and all(c.get("exitCode") == 0 for c in containers),
                "Source repair failed; inspect the simulator CloudWatch log",
            )
            return True

        wait_for(done, "Source repair", attempts=180)
        stream = f'{options["awslogs-stream-prefix"]}/simulator/{task_arn.rsplit("/", 1)[-1]}'

        def receipt():
            token = None
            while True:
                params = dict(
                    logGroupName=options["awslogs-group"],
                    logStreamName=stream,
                    startFromHead=True,
                )
                if token:
                    params["nextToken"] = token
                try:
                    result = self.logs.get_log_events(**params)
                except ClientError as exc:
                    if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                        return False
                    raise
                for event in result["events"]:
                    if "REPAIR_RECEIPT " in event["message"]:
                        return json.loads(
                            event["message"].split("REPAIR_RECEIPT ", 1)[1]
                        )
                next_token = result["nextForwardToken"]
                if next_token == token:
                    return False
                token = next_token

        return wait_for(receipt, "Source repair receipt", attempts=12, interval=5)

    def snapshot_and_clear(self, journal):
        # Save current version IDs before adding delete markers. Never delete versions.
        require(
            self.s3.get_bucket_versioning(**self.owner).get("Status") == "Enabled",
            "Bronze versioning must remain enabled",
        )
        inventory = []
        for table in RELOAD:
            prefix = f"raw/public/{table}/"
            for page in self.s3.get_paginator("list_object_versions").paginate(
                **self.owner, Prefix=prefix
            ):
                for item in page.get("Versions", []):
                    if item["IsLatest"]:
                        inventory.append(
                            {"Key": item["Key"], "VersionId": item["VersionId"]}
                        )
        self.write_json(
            f'metadata/recovery/backups/{journal["recovery_id"]}/{time.time_ns()}.json',
            inventory,
        )
        for table in RELOAD:
            prefix = f"raw/public/{table}/"
            for _ in range(10000):
                objects = self.s3.list_objects_v2(
                    **self.owner, Prefix=prefix, MaxKeys=1000
                ).get("Contents", [])
                if not objects:
                    break
                require(
                    all(x["Key"].startswith(prefix) for x in objects),
                    "Unexpected cleanup key",
                )
                result = self.s3.delete_objects(
                    **self.owner,
                    Delete={
                        "Objects": [{"Key": x["Key"]} for x in objects],
                        "Quiet": True,
                    },
                )
                require(
                    not result.get("Errors"),
                    "Bronze cleanup failed; original versions are retained",
                )
            else:
                raise RuntimeError("Bronze cleanup exceeded its bound")

    def manifest(self):
        import pyarrow.parquet as pq

        rows = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            **self.owner, Prefix="raw/public/seed_manifest/"
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    body = self.s3.get_object(**self.owner, Key=obj["Key"])[
                        "Body"
                    ].read()
                    rows.extend(pq.read_table(io.BytesIO(body)).to_pylist())
        require(len(rows) == 1, "Expected one reloaded Bronze manifest row")
        row = rows[0]
        return {
            key: json.loads(row[key]) if isinstance(row[key], str) else row[key]
            for key in ("specification", "row_counts")
        } | {"seed_id": row["seed_id"], "content_sha256": row["content_sha256"]}

    def repair(self, image, revision):
        marker = self.preflight()
        self.stop_dms()
        journal = self.read_json(JOURNAL, optional=True)
        if journal and journal["stage"] in (
            "bronze-refreshed",
            "silver-done",
            "gold-done",
        ):
            require(
                self.manifest() == journal["receipt"],
                "Bronze no longer matches the recovery checkpoint",
            )
            print(
                "Verified completed Bronze repair; continuing downstream.", flush=True
            )
            return
        if not journal:
            journal = {
                "recovery_id": now().strftime("%Y%m%dT%H%M%S"),
                "profile": marker["profile"],
                "revision": revision,
            }
            self.checkpoint(journal, "pending")
        # Re-running the source command is safe even after a lost response: it
        # verifies every row and accepts only exact canonical v1 or v2 data.
        if journal["stage"] not in ("source-repaired", "bronze-refreshing"):
            journal["receipt"] = self.source_repair(image, journal)
            require(
                journal["receipt"]["specification"]["profile"] == marker["profile"],
                "Source profile mismatch",
            )
            self.checkpoint(journal, "source-repaired")
        self.checkpoint(journal, "bronze-refreshing")
        # Resume the existing task and reload only these tables. Migration CDC
        # is removed below after DMS stops, leaving the verified full snapshot.
        arn = self.dms_task()["ReplicationTaskArn"]
        self.snapshot_and_clear(journal)
        self.dms.start_replication_task(
            ReplicationTaskArn=arn, StartReplicationTaskType="resume-processing"
        )
        wait_for(lambda: self.dms_task()["Status"] == "running", "DMS resume")
        requested = now().replace(microsecond=0)
        try:
            self.dms.reload_tables(
                ReplicationTaskArn=arn,
                TablesToReload=[
                    {"SchemaName": "public", "TableName": x} for x in RELOAD
                ],
                ReloadOption="data-reload",
            )
            counts = {**journal["receipt"]["row_counts"], "seed_manifest": 1}

            def ready():
                task = self.dms_task()
                require(
                    task["Status"] in ("running", "starting"),
                    "DMS stopped or failed during reload",
                )
                stats = [
                    x
                    for page in self.dms.get_paginator(
                        "describe_table_statistics"
                    ).paginate(ReplicationTaskArn=arn)
                    for x in page["TableStatistics"]
                ]
                return reload_complete(stats, counts, requested)

            wait_for(ready, "Targeted DMS reload", attempts=120)
        finally:
            self.stop_dms()
        # Keep only the target full-load files. CDC files may include the source
        # migration (notably the manifest PK change); they are not this snapshot.
        for table in RELOAD:
            prefix = f"raw/public/{table}/"
            files = [
                x
                for p in self.s3.get_paginator("list_objects_v2").paginate(
                    **self.owner, Prefix=prefix
                )
                for x in p.get("Contents", [])
            ]
            full_load = [
                x
                for x in files
                if x["Key"].rsplit("/", 1)[-1].startswith("LOAD")
                and x["Key"].endswith(".parquet")
            ]
            require(
                full_load and all(x["LastModified"] >= requested for x in full_load),
                "Current full-load files are missing",
            )
            import pyarrow.parquet as pq

            loaded_rows = 0
            for obj in full_load:
                body = self.s3.get_object(**self.owner, Key=obj["Key"])["Body"].read()
                parquet = pq.ParquetFile(io.BytesIO(body))
                loaded_rows += parquet.metadata.num_rows
                if table == "payments":
                    require(
                        set(parquet.read(columns=["method"])["method"].to_pylist())
                        == {"credit_card"},
                        "Reloaded payment methods are not canonical",
                    )
            require(
                loaded_rows == counts[table],
                "Bronze full-load row count differs from source",
            )
            cdc = [{"Key": x["Key"]} for x in files if x not in full_load]
            for start in range(0, len(cdc), 1000):
                require(
                    not self.s3.delete_objects(
                        **self.owner,
                        Delete={"Objects": cdc[start : start + 1000], "Quiet": True},
                    ).get("Errors"),
                    "CDC snapshot cleanup failed",
                )
        require(
            self.manifest() == journal["receipt"],
            "Reloaded Bronze manifest differs from verified source",
        )
        marker.update(
            generator_version="customer-history-v2",
            seed_id=journal["receipt"]["seed_id"],
            row_counts=journal["receipt"]["row_counts"],
            checked_at=now().isoformat(),
        )
        self.write_json("metadata/seed-ready.json", marker)
        self.checkpoint(journal, "bronze-refreshed")

    def upload_code(self, glue_dir, dbt_dir):
        scripts_bucket = f"{self.prefix}-{self.account}-glue-scripts"
        for path in (glue_dir / "jobs").glob("*.py"):
            self.s3.upload_file(
                str(path),
                scripts_bucket,
                f"glue-scripts/{path.name}",
                ExtraArgs={"ExpectedBucketOwner": self.account},
            )
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
            for path in (glue_dir / "lib").rglob("*.py"):
                bundle.write(path, str(path.relative_to(glue_dir)))
        self.s3.put_object(
            Bucket=scripts_bucket,
            ExpectedBucketOwner=self.account,
            Key="glue-scripts/lib.zip",
            Body=archive.getvalue(),
        )
        uploaded = set()
        for path in dbt_dir.rglob("*"):
            relative = path.relative_to(dbt_dir)
            if path.is_file() and not any(
                x.startswith(".")
                or x in ("target", "logs", "dbt_packages", "__pycache__")
                for x in relative.parts
            ):
                uploaded.add(f"dbt/platform-dbt-analytics/{relative}")
                self.s3.upload_file(
                    str(path),
                    self.bucket,
                    f"dbt/platform-dbt-analytics/{relative}",
                    ExtraArgs={"ExpectedBucketOwner": self.account},
                )

        prefix = "dbt/platform-dbt-analytics/"
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            **self.owner, Prefix=prefix
        ):
            stale = [
                {"Key": x["Key"]}
                for x in page.get("Contents", [])
                if x["Key"] not in uploaded
            ]
            if stale:
                require(
                    not self.s3.delete_objects(
                        **self.owner, Delete={"Objects": stale, "Quiet": True}
                    ).get("Errors"),
                    "Stale dbt code cleanup failed",
                )

    def run_glue(self, table):
        job = f"{self.prefix}-{table}"
        run = self.glue.start_job_run(JobName=job)["JobRunId"]

        def ready():
            state = self.glue.get_job_run(JobName=job, RunId=run)["JobRun"][
                "JobRunState"
            ]
            require(
                state in ACTIVE | {"SUCCEEDED"},
                "Glue processing failed; inspect its private CloudWatch logs",
            )
            return state == "SUCCEEDED"

        wait_for(ready, "Glue processing", attempts=180)

    def crawl(self):
        name = f"{self.prefix}-silver-crawler"
        requested = now().replace(microsecond=0)
        self.glue.start_crawler(Name=name)

        def ready():
            crawler = self.glue.get_crawler(Name=name)["Crawler"]
            last = crawler.get("LastCrawl", {})
            if (
                crawler["State"] != "READY"
                or last.get("StartTime", datetime.min.replace(tzinfo=timezone.utc))
                < requested
            ):
                return False
            require(last.get("Status") == "SUCCEEDED", "Silver crawler failed")
            return True

        wait_for(ready, "Silver crawler")

    def process(self, scope, silver, glue_dir, dbt_dir, revisions):
        self.preflight()
        require(
            self.dms_task()["Status"] == "stopped",
            "DMS must be stopped before processing a seed snapshot",
        )
        journal = self.read_json(JOURNAL, optional=True)
        require(
            not journal
            or journal["stage"] in ("bronze-refreshed", "silver-done", "gold-done"),
            "An unfinished source repair must be resumed first",
        )
        if scope == "repair-seed-payments":
            require(
                journal is not None and self.manifest() == journal["receipt"],
                "Verified Bronze repair required",
            )
            silver = "fact_payments"
        require(silver in TABLES, "Invalid Silver table")
        self.upload_code(glue_dir, dbt_dir)
        skip_silver = (
            scope == "repair-seed-payments"
            and journal["stage"] in ("silver-done", "gold-done")
            and journal.get("glue_revision") == revisions["glue"]
        )
        if scope != "gold" and not skip_silver:
            self.run_glue(silver)
            self.crawl()
            if scope == "repair-seed-payments":
                journal["glue_revision"] = revisions["glue"]
                self.checkpoint(journal, "silver-done")
        self.run_glue("run-dbt")
        if scope == "repair-seed-payments":
            journal["dbt_revision"] = revisions["dbt"]
            self.checkpoint(journal, "gold-done")
        print("Recovery processing and dbt validation succeeded.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("preflight", "repair", "process"))
    args = parser.parse_args()
    recovery = Recovery(os.environ["ENV"], os.environ["ACCOUNT_ID"])
    if args.phase == "preflight":
        recovery.preflight()
        print("Existing session is ready for recovery.")
    elif args.phase == "repair":
        recovery.repair(os.environ["REPAIR_IMAGE"], os.environ["SIMULATOR_SHA"])
    else:
        recovery.process(
            os.environ["RECOVERY_SCOPE"],
            os.environ["SILVER_TABLE"],
            Path("glue"),
            Path("dbt"),
            {"glue": os.environ["GLUE_SHA"], "dbt": os.environ["DBT_SHA"]},
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Own RuntimeErrors have neutral actionable messages; boto errors can
        # contain credentials/resource addresses and must never be echoed.
        detail = str(exc) if type(exc) is RuntimeError else type(exc).__name__
        print(
            f"Recovery stopped: {detail}. Existing infrastructure remains in place.",
            flush=True,
        )
        raise SystemExit(1) from None
