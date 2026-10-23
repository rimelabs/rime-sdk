"""Check that the required CI job rejects incomplete or failed package checks."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / "workflows/checks.yml"
PACKAGES = ("python", "typescript", "go", "rust")


@pytest.fixture
def gate():
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    gate = jobs["ci-passed"]
    # A new package job must also become a dependency of the required check.
    assert set(gate["needs"]) == set(jobs) - {"ci-passed"}
    assert gate["name"] == "CI passed"
    assert gate["if"] == "${{ always() }}"
    step = gate["steps"][0]
    assert step["env"]["JOB_RESULTS"] == "${{ toJSON(needs) }}"
    return step["run"]


@pytest.fixture
def results():
    return {
        "changes": {
            "result": "success",
            "outputs": dict.fromkeys(PACKAGES, "true"),
        },
        **{package: {"result": "success"} for package in PACKAGES},
    }


def run_gate(script, results):
    return subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        env=dict(os.environ, JOB_RESULTS=json.dumps(results)),
        text=True,
        capture_output=True,
        check=False,
    )


def test_all_packages_pass(gate, results):
    assert run_gate(gate, results).returncode == 0


@pytest.mark.parametrize("required_package", [None, *PACKAGES])
def test_unchanged_packages_can_be_skipped(gate, results, required_package):
    for package in PACKAGES:
        if package != required_package:
            results["changes"]["outputs"][package] = "false"
            results[package]["result"] = "skipped"
    assert run_gate(gate, results).returncode == 0


@pytest.mark.parametrize("package", PACKAGES)
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_changed_package_must_succeed(gate, results, package, result):
    results[package]["result"] = result
    assert run_gate(gate, results).returncode != 0


@pytest.mark.parametrize("result", ["failure", "cancelled"])
def test_unchanged_package_failure_still_blocks_merge(gate, results, result):
    results["changes"]["outputs"]["go"] = "false"
    results["go"]["result"] = result
    assert run_gate(gate, results).returncode != 0


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_change_detection_must_succeed(gate, results, result):
    results["changes"]["result"] = result
    assert run_gate(gate, results).returncode != 0


def test_missing_change_output_blocks_merge(gate, results):
    del results["changes"]["outputs"]["rust"]
    assert run_gate(gate, results).returncode != 0


def test_missing_package_result_blocks_merge(gate, results):
    del results["rust"]
    assert run_gate(gate, results).returncode != 0
