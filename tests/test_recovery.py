import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from session_recover import Recovery, RELOAD, reload_complete


def recovery():
    clients = {
        name: MagicMock()
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
    return Recovery("dev", "000000000000", clients)


def test_stale_reload_and_wrong_counts_never_pass():
    requested = datetime.now(timezone.utc)
    counts = dict.fromkeys(RELOAD, 1)
    stats = [
        dict(
            SchemaName="public",
            TableName=t,
            TableState="Table completed",
            FullLoadStartTime=requested,
            FullLoadRows=1,
        )
        for t in RELOAD
    ]
    assert reload_complete(stats, counts, requested)
    stats[0]["FullLoadStartTime"] -= timedelta(seconds=1)
    assert not reload_complete(stats, counts, requested)
    stats[0]["FullLoadStartTime"] = requested
    stats[0]["FullLoadRows"] = 2
    assert not reload_complete(stats, counts, requested)


def test_cleanup_preserves_versions_and_only_three_tables():
    r = recovery()
    r.s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
    r.s3.get_paginator.return_value.paginate.return_value = [
        {
            "Versions": [
                {
                    "Key": "raw/public/payments/LOAD1.parquet",
                    "VersionId": "old",
                    "IsLatest": True,
                }
            ]
        }
    ]
    r.s3.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "raw/public/payments/LOAD1.parquet"}]},
        {},
        {},
        {},
    ]
    r.s3.delete_objects.return_value = {}
    r.snapshot_and_clear({"recovery_id": "test"})
    deletes = r.s3.delete_objects.call_args_list
    assert len(deletes) == 1
    assert deletes[0].kwargs["Delete"]["Objects"] == [
        {"Key": "raw/public/payments/LOAD1.parquet"}
    ]
    prefixes = {call.kwargs["Prefix"] for call in r.s3.list_objects_v2.call_args_list}
    assert prefixes == {f"raw/public/{t}/" for t in RELOAD}
    backup = json.loads(r.s3.put_object.call_args.kwargs["Body"])
    assert backup[0]["VersionId"] == "old"


def test_cleanup_stops_on_partial_delete_failure():
    r = recovery()
    r.s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
    r.s3.get_paginator.return_value.paginate.return_value = []
    r.s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "raw/public/payments/LOAD1.parquet"}]
    }
    r.s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied"}]}
    with pytest.raises(RuntimeError, match="cleanup failed"):
        r.snapshot_and_clear({"recovery_id": "test"})
    assert r.s3.delete_objects.call_count == 1


def test_failed_crawler_ready_is_not_success():
    r = recovery()
    r.glue.get_crawler.return_value = {
        "Crawler": {
            "State": "READY",
            "LastCrawl": {
                "Status": "FAILED",
                "StartTime": datetime.now(timezone.utc) + timedelta(seconds=10),
            },
        }
    }
    with pytest.raises(RuntimeError, match="crawler failed"):
        r.crawl()


def test_gold_only_runs_gold_and_blocks_incomplete_repairs():
    r = recovery()
    r.preflight = MagicMock()
    r.dms_task = MagicMock(return_value={"Status": "stopped"})
    r.read_json = MagicMock(return_value=None)
    r.upload_code = MagicMock()
    r.run_glue = MagicMock()
    r.crawl = MagicMock()
    r.process("gold", "fact_payments", Path("."), Path("."), {"glue": "a", "dbt": "b"})
    r.run_glue.assert_called_once_with("run-dbt")
    r.crawl.assert_not_called()
    r.ecs.run_task.assert_not_called()
    r.dms.reload_tables.assert_not_called()
    r.read_json.return_value = {"stage": "bronze-refreshing"}
    with pytest.raises(RuntimeError, match="unfinished source repair"):
        r.process(
            "gold", "fact_payments", Path("."), Path("."), {"glue": "a", "dbt": "b"}
        )


def test_repair_resume_skips_successful_source_and_bronze():
    r = recovery()
    r.preflight = MagicMock(return_value={})
    r.stop_dms = MagicMock()
    r.read_json = MagicMock(
        return_value={"stage": "silver-done", "receipt": {"seed_id": "verified"}}
    )
    r.manifest = MagicMock(return_value={"seed_id": "verified"})
    r.repair("image", "revision")
    r.ecs.run_task.assert_not_called()
    r.dms.reload_tables.assert_not_called()


def test_processing_resume_skips_silver_only_for_same_code():
    r = recovery()
    r.preflight = MagicMock()
    r.dms_task = MagicMock(return_value={"Status": "stopped"})
    journal = {"stage": "silver-done", "receipt": {}, "glue_revision": "same"}
    r.read_json = MagicMock(return_value=journal)
    r.manifest = MagicMock(return_value={})
    r.upload_code = MagicMock()
    r.run_glue = MagicMock()
    r.crawl = MagicMock()
    r.checkpoint = MagicMock()
    r.process(
        "repair-seed-payments",
        "fact_payments",
        Path("."),
        Path("."),
        {"glue": "same", "dbt": "b"},
    )
    r.run_glue.assert_called_once_with("run-dbt")
    r.run_glue.reset_mock()
    r.process(
        "repair-seed-payments",
        "fact_payments",
        Path("."),
        Path("."),
        {"glue": "changed", "dbt": "b"},
    )
    assert [c.args[0] for c in r.run_glue.call_args_list] == [
        "fact_payments",
        "run-dbt",
    ]
