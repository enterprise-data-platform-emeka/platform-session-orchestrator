import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from session_data import bucket_names, empty_bucket
from seed_readiness import TABLES, complete

ACCOUNT = "1" * 12


def empty_client():
    s3 = MagicMock()
    s3.list_object_versions.return_value = {}
    s3.list_objects_v2.return_value = {}
    s3.list_multipart_uploads.return_value = {}
    s3.delete_objects.return_value = {}
    return s3


def test_scope_excludes_backend():
    names = bucket_names("dev", ACCOUNT)
    assert len(names) == 7
    assert all(x.startswith(f"edp-dev-{ACCOUNT}-") for x in names)
    assert not any("tfstate" in x or "bootstrap" in x for x in names)
    with pytest.raises(ValueError):
        bucket_names("../prod", ACCOUNT)


def test_versions_markers_multipart_and_verification():
    s3 = empty_client()
    s3.list_object_versions.side_effect = [
        {
            "Versions": [{"Key": "data", "VersionId": "v1"}],
            "DeleteMarkers": [{"Key": "data", "VersionId": "v2"}],
        },
        {"Versions": [{"Key": "older", "VersionId": "v3"}]},
        {},
        {},
    ]
    s3.list_multipart_uploads.side_effect = [
        {"Uploads": [{"Key": "partial", "UploadId": "u1"}]},
        {},
        {},
    ]
    empty_bucket(s3, "test-bucket", ACCOUNT)
    assert s3.delete_objects.call_count == 2
    assert s3.delete_objects.call_args_list[0].kwargs["Delete"]["Objects"] == [
        {"Key": "data", "VersionId": "v1"},
        {"Key": "data", "VersionId": "v2"},
    ]
    assert s3.abort_multipart_upload.call_count == 1
    assert all(
        c.kwargs["ExpectedBucketOwner"] == ACCOUNT
        for c in s3.list_object_versions.call_args_list
    )
    s3.delete_bucket.assert_not_called()


def test_delete_failure_is_not_success():
    s3 = empty_client()
    s3.list_object_versions.return_value = {
        "Versions": [{"Key": "data", "VersionId": "v1"}]
    }
    s3.delete_objects.return_value = {"Errors": [{"Code": "AccessDenied"}]}
    with pytest.raises(RuntimeError):
        empty_bucket(s3, "test-bucket", ACCOUNT)


def test_missing_bucket_ok_but_denied_fails():
    s3 = empty_client()
    s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")
    empty_bucket(s3, "test-bucket", ACCOUNT)
    s3.head_bucket.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadBucket")
    with pytest.raises(ClientError):
        empty_bucket(s3, "test-bucket", ACCOUNT)


def test_late_writer_fails_verification():
    s3 = empty_client()
    s3.list_object_versions.side_effect = [
        {},
        {"Versions": [{"Key": "late", "VersionId": "v1"}]},
    ]
    with pytest.raises(RuntimeError):
        empty_bucket(s3, "test-bucket", ACCOUNT)


def test_full_load_gate():
    stats = [
        {
            "SchemaName": "public",
            "TableName": name,
            "TableState": "Table completed",
            "FullLoadRows": 10,
            "FullLoadErrorRows": 0,
        }
        for name in TABLES
    ]
    assert complete(stats, {"orders": 10})
    assert not complete(stats, {"orders": 11})
    assert not complete(stats[:-1], {"orders": 10})
    stats[0]["TableState"] = "Before load"
    assert not complete(stats, {"orders": 10})
    stats[0]["TableState"] = "Table completed"
    stats[0]["FullLoadErrorRows"] = 1
    assert not complete(stats, {"orders": 10})
