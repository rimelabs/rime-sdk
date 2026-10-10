"""Check API updates, retries, and recovery from old release notifications."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "update-api-dependencies.py"
SPEC = importlib.util.spec_from_file_location("api_updates", SCRIPT)
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


@pytest.fixture
def repository(tmp_path):
    for directory in ["python", "typescript", "go", "rust"]:
        (tmp_path / directory).mkdir()
    (tmp_path / "python/pyproject.toml").write_text(
        '[project]\nversion = "0.3.0"\ndependencies = ["rime-api==0.3.0", "other==0.3.0"]\n'
    )
    (tmp_path / "typescript/package.json").write_text(
        json.dumps({"dependencies": {"@rimelabs/api": "0.3.0"}})
    )
    (tmp_path / "go/go.mod").write_text(
        "module test\n\ngo 1.24.0\n\nrequire (\n\tgithub.com/rimelabs/rime-api/go v0.3.0\n)\n"
    )
    (tmp_path / "rust/Cargo.toml").write_text(
        '[package]\nversion = "0.3.0"\n[dependencies]\nrimelabs-api = "0.3.0"\nother = "0.3.0"\n'
    )
    return tmp_path


def test_updates_api_only_and_resolves_every_lockfile(repository, monkeypatch):
    commands = []
    monkeypatch.setattr(
        updater, "run", lambda command, directory: commands.append((command, directory))
    )
    updater.update(repository, "0.4.0")
    python = (repository / "python/pyproject.toml").read_text()
    rust = (repository / "rust/Cargo.toml").read_text()
    assert '"rime-api==0.4.0"' in python
    assert '"other==0.3.0"' in python
    assert 'rimelabs-api = "0.4.0"' in rust
    assert 'other = "0.3.0"' in rust
    assert 'version = "0.3.0"' in python and 'version = "0.3.0"' in rust
    assert {command[0] for command, _ in commands} == {"uv", "npm", "go", "cargo"}
    npm = next(command for command, _ in commands if command[0] == "npm")
    assert "--package-lock-only" in npm and "--ignore-scripts" in npm
    assert npm[-1] == "@rimelabs/api@0.4.0"
    assert (
        ["go", "get", "github.com/rimelabs/rime-api/go@v0.4.0"],
        repository / "go",
    ) in commands
    assert commands[-1][0][-2:] == ["--precise", "0.4.0"]


@pytest.mark.parametrize("version", ["0.3.0", "0.2.0"])
def test_repeated_or_old_release_does_not_change_files(repository, monkeypatch, version):
    before = {path: path.read_bytes() for path in repository.rglob("*") if path.is_file()}
    monkeypatch.setattr(updater, "run", lambda *_: pytest.fail("No package manager should run"))
    updater.update(repository, version)
    assert before == {path: path.read_bytes() for path in before}


@pytest.mark.parametrize(
    "version", ["v0.4.0", "0.4", "0.4.0-alpha.1", "01.2.3", "0.4.0\n", "0.4.0;exit"]
)
def test_rejects_invalid_version_before_accessing_repository(tmp_path, version):
    with pytest.raises(ValueError, match="stable API version"):
        updater.update(tmp_path, version)


def test_old_release_cannot_downgrade_one_language(repository, monkeypatch):
    (repository / "typescript/package.json").write_text(
        json.dumps({"dependencies": {"@rimelabs/api": "0.5.0"}})
    )
    monkeypatch.setattr(updater, "run", lambda *_: pytest.fail("Old notifications must be ignored"))
    updater.update(repository, "0.4.0")
    assert "rime-api==0.3.0" in (repository / "python/pyproject.toml").read_text()


def test_retries_index_delay_then_stops_on_success(tmp_path, monkeypatch):
    attempts = []

    def command(*args, **kwargs):
        attempts.append(args)
        if len(attempts) < 3:
            raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr(updater.subprocess, "run", command)
    monkeypatch.setattr(updater.time, "sleep", lambda _: None)
    updater.run(["uv", "lock"], tmp_path)
    assert len(attempts) == 3


def test_failed_resolution_stops_update(repository, monkeypatch):
    attempts = []

    def command(*args, **kwargs):
        attempts.append(args[0])
        raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr(updater.subprocess, "run", command)
    monkeypatch.setattr(updater.time, "sleep", lambda _: None)
    with pytest.raises(subprocess.CalledProcessError):
        updater.update(repository, "0.4.0")
    assert len(attempts) == 5
    assert all(command[0] == "uv" for command in attempts)
