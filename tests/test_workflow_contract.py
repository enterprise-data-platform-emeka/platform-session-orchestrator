from pathlib import Path
import subprocess
import yaml

ROOT = Path(__file__).resolve().parents[1]


def workflow(name):
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


def test_modes_and_teardown():
    start = workflow("session-start.yml")
    destroy = workflow("session-destroy.yml")
    inputs = start.get("on", start.get(True))["workflow_dispatch"]["inputs"]
    assert inputs["data_mode"]["default"] == "seed-only"
    assert inputs["seed_profile"]["default"] == "customer-intelligence-36m"
    assert start["concurrency"] == destroy["concurrency"]
    source = {s.get("name"): s for s in start["jobs"]["cdc-source-ready"]["steps"]}
    assert "== 'seed-and-live'" in source["Start bounded live injection task"]["if"]
    assert "--stop-after-load" in source["Validate current seed full load"]["run"]
    steps = {s.get("name"): s for s in destroy["jobs"]["terraform-destroy"]["steps"]}
    assert "outcome == 'success'" in steps["Empty all session S3 data and verify"]["if"]
    assert "empty --environment" in steps["Empty all session S3 data and verify"]["run"]


def test_shell_syntax():
    for name in ("session-start.yml", "session-destroy.yml"):
        for job in workflow(name)["jobs"].values():
            for step in job["steps"]:
                if "run" in step:
                    result = subprocess.run(
                        ["bash", "-n"],
                        input=step["run"],
                        text=True,
                        capture_output=True,
                    )
                    assert result.returncode == 0, (step["name"], result.stderr)
