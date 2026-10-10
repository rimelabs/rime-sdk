"""Update the four SDKs to a published, stable API version."""

import argparse
import json
import re
import subprocess
import time
import tomllib
from pathlib import Path

VERSION = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"


def version_tuple(version: str) -> tuple[int, ...]:
    if not re.fullmatch(VERSION, version):
        raise ValueError(f"Expected a stable API version such as 0.4.0, got {version!r}")
    return tuple(map(int, version.split(".")))


def run(command: list[str], directory: Path) -> None:
    # A release can reach the registries before all indexes have refreshed.
    for attempt in range(5):
        try:
            subprocess.run(command, cwd=directory, check=True)
            return
        except subprocess.CalledProcessError:
            if attempt == 4:
                raise
            time.sleep(15)


def update(root: Path, version: str) -> None:
    requested = version_tuple(version)
    python_path = root / "python/pyproject.toml"
    typescript_path = root / "typescript/package.json"
    rust_path = root / "rust/Cargo.toml"
    go_path = root / "go/go.mod"
    python_text = python_path.read_text()
    rust_text = rust_path.read_text()
    go_text = go_path.read_text()
    python_dependencies = tomllib.loads(python_text)["project"]["dependencies"]
    (python_version,) = [
        item.removeprefix("rime-api==")
        for item in python_dependencies
        if item.startswith("rime-api==")
    ]
    typescript_version = json.loads(typescript_path.read_text())["dependencies"]["@rimelabs/api"]
    rust_version = tomllib.loads(rust_text)["dependencies"]["rimelabs-api"]
    (go_version,) = re.findall(
        r"^\s*github\.com/rimelabs/rime-api/go v(\S+)\s*$", go_text, re.MULTILINE
    )
    current = [
        version_tuple(value)
        for value in [python_version, typescript_version, rust_version, go_version]
    ]
    if any(value > requested for value in current):
        print("A newer API dependency is already on this branch. Skipping the old release.")
        return
    if all(value == requested for value in current):
        print("All four SDKs already use this API version.")
        return

    # Validate each replacement before changing any file.
    python_updated, python_count = re.subn(
        rf'"rime-api=={re.escape(python_version)}"',
        f'"rime-api=={version}"',
        python_text,
    )
    rust_updated, rust_count = re.subn(
        rf'^rimelabs-api = "{re.escape(rust_version)}"$',
        f'rimelabs-api = "{version}"',
        rust_text,
        flags=re.MULTILINE,
    )
    if python_count != 1 or rust_count != 1:
        raise ValueError("Expected one API dependency in each Python and Rust manifest")
    python_path.write_text(python_updated)
    rust_path.write_text(rust_updated)
    run(["uv", "lock", "--project", "python", "--upgrade-package", "rime-api"], root)
    run(
        [
            "npm",
            "install",
            "--package-lock-only",
            "--ignore-scripts",
            "--save-exact",
            "--workspace",
            "@rimelabs/sdk",
            f"@rimelabs/api@{version}",
        ],
        root,
    )
    run(["go", "get", f"github.com/rimelabs/rime-api/go@v{version}"], root / "go")
    run(["go", "mod", "tidy"], root / "go")
    run(
        [
            "cargo",
            "update",
            "--manifest-path",
            "rust/Cargo.toml",
            "--package",
            "rimelabs-api",
            "--precise",
            version,
        ],
        root,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    arguments = parser.parse_args()
    update(Path(__file__).resolve().parents[2], arguments.version)
