"""Check release recovery and registry failures without publishing packages."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "update_examples", Path(__file__).parents[1] / "update-example-dependency.py"
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)


def manifest(language, version):
    if language == "python":
        return f'[project]\nname = "examples"\ndependencies = ["rimelabs-sdk=={version}"]\n'
    return json.dumps({"dependencies": {"@rimelabs/sdk": version}})


def project(root, language, version):
    path = root / updater.manifest_path(language)
    path.parent.mkdir(parents=True)
    path.write_text(manifest(language, version))
    return path


@pytest.mark.parametrize("language", ["python", "typescript"])
@pytest.mark.parametrize("requested", ["0.1.0-alpha.6", "0.1.0-alpha.7"])
def test_recovery_does_not_downgrade_or_rewrite_current_pin(
    tmp_path, monkeypatch, language, requested
):
    path = project(tmp_path, language, "0.1.0-alpha.7")
    before = path.read_bytes()
    monkeypatch.setattr(updater.subprocess, "run", lambda *a, **k: pytest.fail("No command needed"))
    assert not updater.update(tmp_path, language, requested)
    assert path.read_bytes() == before


@pytest.mark.parametrize("language", ["python", "typescript"])
def test_recovery_preserves_a_newer_pending_pr(tmp_path, monkeypatch, language):
    path = project(tmp_path, language, "0.1.0-alpha.6")
    before = path.read_bytes()

    def run(command, **kwargs):
        assert command[0] == "git"  # Dependency resolution must not run.
        return subprocess.CompletedProcess(command, 0, manifest(language, "0.1.0-alpha.8"))

    monkeypatch.setattr(updater.subprocess, "run", run)
    assert not updater.update(tmp_path, language, "0.1.0-alpha.7")
    assert path.read_bytes() == before


@pytest.mark.parametrize("language", ["python", "typescript"])
@pytest.mark.parametrize("pending_branch", [True, False])
@pytest.mark.parametrize(
    "outcome", ["published", "missing", "local-link", "wrong-version", "other-index"]
)
def test_update_requires_the_requested_registry_package(
    tmp_path, monkeypatch, language, outcome, pending_branch
):
    path = project(tmp_path, language, "0.1.0-alpha.6")
    commands = []
    delays = []
    monkeypatch.setattr(updater.time, "sleep", delays.append)

    def run(command, **kwargs):
        commands.append(command)
        if command[0] == "git":
            if not pending_branch:
                return subprocess.CompletedProcess(command, 128, "", "Unknown revision")
            # Retry an existing PR with the same target version.
            return subprocess.CompletedProcess(command, 0, manifest(language, "0.1.0-alpha.7"))
        if outcome == "missing":
            raise subprocess.CalledProcessError(1, command)
        version = "0.1.0a7" if language == "python" else "0.1.0-alpha.7"
        path.write_text(manifest(language, version))
        locked = "0.1.0" if outcome == "wrong-version" else version
        if language == "python":
            source = '{ registry = "https://pypi.org/simple" }'
            if outcome == "local-link":
                source = '{ editable = "../../python" }'
            elif outcome == "other-index":
                source = '{ registry = "https://packages.example.com/simple" }'
            (path.parent / "uv.lock").write_text(
                f'[[package]]\nname = "rimelabs-sdk"\nversion = "{locked}"\nsource = {source}\n'
            )
        else:
            sdk = {
                "version": locked,
                "resolved": "https://registry.npmjs.org/sdk.tgz",
                "integrity": "sha512-test",
            }
            if outcome == "local-link":
                sdk = {"link": True, "resolved": "../../typescript"}
            elif outcome == "other-index":
                sdk["resolved"] = "https://packages.example.com/sdk.tgz"
            (path.parent / "package-lock.json").write_text(
                json.dumps({"packages": {"node_modules/@rimelabs/sdk": sdk}})
            )
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(updater.subprocess, "run", run)
    if outcome == "published":
        assert updater.update(tmp_path, language, "0.1.0-alpha.7")
    elif outcome == "missing":
        with pytest.raises(subprocess.CalledProcessError):
            updater.update(tmp_path, language, "0.1.0-alpha.7")
    else:
        with pytest.raises(ValueError):
            updater.update(tmp_path, language, "0.1.0-alpha.7")
    assert len(delays) == (9 if outcome == "missing" else 0)
    if language == "python":
        assert "rimelabs-sdk==0.1.0a7" in commands[-1]
        assert "--refresh" in commands[-1]
        assert commands[-1][commands[-1].index("--default-index") + 1] == "https://pypi.org/simple"
    else:
        assert "@rimelabs/sdk@0.1.0-alpha.7" in commands[-1]
        assert "--prefer-online" in commands[-1]
        assert "--@rimelabs:registry=https://registry.npmjs.org/" in commands[-1]


def test_registry_update_recovers_after_publication_delay(tmp_path, monkeypatch):
    calls = []
    delays = []
    command = ["package-manager", "install", "new-release"]

    def run(arguments, **kwargs):
        assert arguments == command
        assert kwargs == {"cwd": tmp_path, "check": True}
        calls.append(arguments)
        if len(calls) < 3:
            raise subprocess.CalledProcessError(1, arguments)
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(updater.subprocess, "run", run)
    monkeypatch.setattr(updater.time, "sleep", delays.append)
    updater.run_registry_update(command, tmp_path)
    assert len(calls) == 3
    assert delays == [15, 15]
