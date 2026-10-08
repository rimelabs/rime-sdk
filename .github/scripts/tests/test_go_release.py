"""Check public Go release verification without publishing a version."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "go/tools/check-public-release.sh"
MODULE = "github.com/rimelabs/rime-sdk/go"


@pytest.fixture
def runner(tmp_path):
    commands = tmp_path / "commands.jsonl"
    binary = tmp_path / "bin"
    binary.mkdir()
    fake_go = binary / "go"
    fake_go.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "log = Path(os.environ['COMMAND_LOG'])\n"
        "previous = log.read_text().splitlines() if log.exists() else []\n"
        "args = sys.argv[1:]\n"
        "with log.open('a') as output:\n"
        "    output.write(json.dumps({'args': args, 'env': dict(os.environ), "
        "'cwd': os.getcwd()}) + '\\n')\n"
        "if args[:2] == ['mod', 'download']:\n"
        "    attempts = sum(json.loads(row)['args'][:2] == ['mod', 'download'] "
        "for row in previous)\n"
        "    sys.exit(int(attempts < int(os.environ['FAIL_DOWNLOADS'])))\n"
        "if args == ['run', '.']:\n"
        f"    assert '{MODULE}' in Path('main.go').read_text()\n"
    )
    fake_go.chmod(0o755)
    sleep = binary / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    sleep.chmod(0o755)

    def run(tag, failures=0):
        env = dict(os.environ)
        env.update(
            PATH=f"{binary}{os.pathsep}{env['PATH']}",
            COMMAND_LOG=str(commands),
            FAIL_DOWNLOADS=str(failures),
            GOPROXY="file:///private-proxy",
            GOSUMDB="off",
            GOPRIVATE="*",
            GONOPROXY="*",
            GONOSUMDB="*",
            GOWORK="/private/workspace/go.work",
            GOFLAGS="-mod=vendor",
        )
        result = subprocess.run(
            ["bash", str(SCRIPT), tag], env=env, text=True, capture_output=True, check=False
        )
        records = (
            [json.loads(row) for row in commands.read_text().splitlines()]
            if commands.exists()
            else []
        )
        return result, records

    return run


def test_public_download_ignores_private_settings_and_retries(runner):
    result, records = runner("go/v0.1.0-alpha.2", failures=2)
    assert result.returncode == 0, result.stderr
    downloads = [r for r in records if r["args"][:2] == ["mod", "download"]]
    assert len(downloads) == 3
    assert all(r["args"][2] == f"{MODULE}@v0.1.0-alpha.2" for r in downloads)
    for record in records:
        env = record["env"]
        assert env["GOPROXY"] == "https://proxy.golang.org"
        assert env["GOSUMDB"] == "sum.golang.org"
        assert env["GOPRIVATE"] == env["GONOPROXY"] == env["GONOSUMDB"] == ""
        assert env["GOWORK"] == env["GOENV"] == "off"
        assert env["GOFLAGS"] == ""
        assert not Path(record["cwd"]).exists(), "Temporary module must be removed"
    assert any(r["args"] == ["run", "."] for r in records)


def test_failed_download_stops_before_install_or_build(runner):
    result, records = runner("go/v0.1.0-alpha.2", failures=100)
    assert result.returncode != 0
    assert "Public download failed" in result.stderr
    assert sum(r["args"][:2] == ["mod", "download"] for r in records) == 8
    assert not any(r["args"][0] in {"get", "run"} for r in records)


@pytest.mark.parametrize("tag", ["go-v0.1.0-alpha.2", "v0.1.0", "python-v0.1.0", "go/v1.2.3;exit"])
def test_rejects_wrong_tag_before_running_go(runner, tag):
    result, records = runner(tag)
    assert result.returncode != 0
    assert "Invalid Go release tag" in result.stderr
    assert not records
