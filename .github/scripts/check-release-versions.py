"""Check package versions against the Release Please manifest before release."""

import json
from pathlib import Path

import tomllib
from packaging.version import Version

root = Path(__file__).resolve().parents[2]
manifest = json.loads((root / ".release-please-manifest.json").read_text())
config = json.loads((root / "release-please-config.json").read_text())
python = tomllib.loads((root / "python/pyproject.toml").read_text())["project"]
lock = tomllib.loads((root / "python/uv.lock").read_text())
node = json.loads((root / "typescript/package.json").read_text())
node_lock = json.loads((root / "typescript/package-lock.json").read_text())

assert Version(python["version"]) == Version(manifest["python"]), (
    "Python manifest mismatch"
)
locked_python = next(
    package for package in lock["package"] if package["name"] == python["name"]
)
assert locked_python["version"] == python["version"], "Python lockfile mismatch"
assert node["version"] == manifest["typescript"], "Node.js manifest mismatch"
assert node_lock["version"] == node["version"], "Node.js lockfile mismatch"
assert node_lock["packages"][""]["version"] == node["version"], (
    "Node.js root lock mismatch"
)

assert (root / "go/version.txt").read_text().strip() == manifest["go"], (
    "Go manifest mismatch"
)

if config["prerelease"]:
    for component, version in manifest.items():
        parsed = Version(version)
        assert parsed.pre and parsed.pre[0] == "a", f"{component} must remain an alpha"

print("Package versions, lockfiles, and release policy agree.")
