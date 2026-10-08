"""Update one example project after its SDK has been published."""

import argparse
import json
import os
import subprocess
import time
import tomllib
from pathlib import Path

from packaging.version import Version


def manifest_path(language):
    filename = "pyproject.toml" if language == "python" else "package.json"
    return Path("examples") / language / filename


def pinned_version(language, text):
    if language == "typescript":
        return Version(json.loads(text)["dependencies"]["@rimelabs/sdk"])
    dependencies = tomllib.loads(text)["project"]["dependencies"]
    pin = next(value for value in dependencies if value.startswith("rimelabs-sdk=="))
    return Version(pin.removeprefix("rimelabs-sdk=="))


def run_registry_update(command, root):
    # Registry indexes can lag behind a successful publish. Refresh metadata on
    # each bounded retry; keep lockfile and registry checks outside this loop.
    for attempt in range(1, 11):
        try:
            subprocess.run(command, cwd=root, check=True)
            return
        except subprocess.CalledProcessError:
            if attempt == 10:
                raise
            print(f"Registry update failed; retrying in 15 seconds ({attempt}/10).", flush=True)
            time.sleep(15)


def update(root, language, version):
    target = Version(version)
    manifest = manifest_path(language)
    current = pinned_version(language, (root / manifest).read_text())
    if target <= current:
        print(f"Examples already use {current}; no update needed for {version}.")
        return False

    # A recovery run must not replace a newer, still-open dependency update.
    pending = subprocess.run(
        ["git", "show", f"origin/automation/examples-{language}:{manifest.as_posix()}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if pending.returncode == 0 and target < pinned_version(language, pending.stdout):
        print("A newer example update already exists; leaving it unchanged.")
        return False

    project = str(manifest.parent)
    if language == "python":
        run_registry_update(
            [
                "uv",
                "add",
                "--project",
                project,
                "--no-sync",
                "--refresh",  # Publication can be newer than the restored uv cache.
                "--default-index",
                "https://pypi.org/simple",
                f"rimelabs-sdk=={target}",
            ],
            root,
        )
        lock = tomllib.loads((root / project / "uv.lock").read_text())
        sdk = next(p for p in lock["package"] if p["name"] == "rimelabs-sdk")
        if sdk["source"].get("registry", "").rstrip("/") != "https://pypi.org/simple":
            raise ValueError("The Python example must use the published PyPI package")
    else:
        run_registry_update(
            [
                "npm",
                "--prefix",
                project,
                "install",
                "--package-lock-only",
                "--ignore-scripts",
                "--no-audit",
                "--no-fund",
                "--save-exact",
                "--prefer-online",
                f"@rimelabs/sdk@{version}",
                "--registry=https://registry.npmjs.org/",
                "--@rimelabs:registry=https://registry.npmjs.org/",
            ],
            root,
        )
        lock = json.loads((root / project / "package-lock.json").read_text())
        sdk = lock["packages"]["node_modules/@rimelabs/sdk"]
        if (
            sdk.get("link")
            or not sdk.get("resolved", "").startswith("https://registry.npmjs.org/")
            or not sdk.get("integrity")
        ):
            raise ValueError("The TypeScript example must use the published npm package")
    if Version(sdk["version"]) != target:
        raise ValueError("The lockfile does not contain the requested SDK version")
    if pinned_version(language, (root / manifest).read_text()) != target:
        raise ValueError("The example does not pin the requested SDK version")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("language", choices=("python", "typescript"))
    parser.add_argument("version")
    args = parser.parse_args()
    changed = update(Path(__file__).resolve().parents[2], args.language, args.version)
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as stream:
            stream.write(f"changed={str(changed).lower()}\n")


if __name__ == "__main__":
    main()
