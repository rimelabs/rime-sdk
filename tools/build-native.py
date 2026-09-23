"""Build local extensions. Run with uv run --no-project tools/build-native.py."""

import argparse
import os
import platform
import shutil
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--test-support", action="store_true")
parser.add_argument("--release", action="store_true")
args = parser.parse_args()
command = ["cargo", "build", "--locked", "-p", "rime-python", "-p", "rime-node"]
if args.test_support:
    command += ["--features", "test-support"]
if args.release:
    command += ["--release"]
subprocess.run(command, cwd=root, check=True)
build = root / "target"
if target := os.environ.get("CARGO_BUILD_TARGET"):
    build /= target
build /= "release" if args.release else "debug"
system = platform.system()
ext = {"Darwin": "dylib", "Linux": "so", "Windows": "dll"}[system]
prefix = "" if system == "Windows" else "lib"
arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "x64", "AMD64": "x64"}[
    platform.machine()
]
node_platform = {"Darwin": "darwin", "Linux": "linux", "Windows": "win32"}[system]
libc = "-gnu" if system == "Linux" else ""
node_dir = root / "typescript/native"
node_dir.mkdir(exist_ok=True)
shutil.copy2(
    build / f"{prefix}rime_node.{ext}",
    node_dir / f"rime-sdk.{node_platform}-{arch}{libc}.node",
)
shutil.copy2(
    build / f"{prefix}_native.{ext}",
    root
    / "python/src/rimelabs_sdk"
    / ("_native.pyd" if system == "Windows" else "_native.abi3.so"),
)
