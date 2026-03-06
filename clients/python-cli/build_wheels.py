#!/usr/bin/env python3
"""
Build platform-specific wheels for the lakefs-cli package.

Each wheel embeds pre-built lakefs and lakectl binaries for a single platform.
Binaries are downloaded from the lakeFS GitHub releases page.

Usage:
    python build_wheels.py <lakefs-version>

Example:
    python build_wheels.py 1.50.0
"""

import argparse
import io
import os
import shutil
import stat
import sys
import tarfile
import tempfile
import urllib.request
import urllib.error
import zipfile
from pathlib import Path

DOWNLOAD_URL = "https://github.com/treeverse/lakeFS/releases/download/"

PLATFORMS = [
    {
        "os": "darwin",
        "arch": "arm64",
        "compression": "tar.gz",
        "wheel_tag": "macosx_11_0_arm64",
    },
    {
        "os": "darwin",
        "arch": "x86_64",
        "compression": "tar.gz",
        "wheel_tag": "macosx_10_9_x86_64",
    },
    {
        "os": "linux",
        "arch": "x86_64",
        "compression": "tar.gz",
        "wheel_tag": "manylinux_2_17_x86_64",
    },
    {
        "os": "linux",
        "arch": "arm64",
        "compression": "tar.gz",
        "wheel_tag": "manylinux_2_17_aarch64",
    },
    {
        "os": "windows",
        "arch": "x86_64",
        "compression": "zip",
        "wheel_tag": "win_amd64",
    },
    {
        "os": "windows",
        "arch": "arm64",
        "compression": "zip",
        "wheel_tag": "win_arm64",
    },
]


def download_file(url: str) -> io.BytesIO:
    """Download a file and return its contents as a BytesIO object."""
    print(f"  Downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "lakefs-cli-builder"})
    try:
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to download {url}: {exc}") from exc
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf


def extract_binaries(content: io.BytesIO, target_dir: str, plat: dict):
    """Extract lakefs and lakectl binaries into *target_dir*."""
    if plat["compression"] == "zip":
        with zipfile.ZipFile(content, "r") as zf:
            zf.extract("lakefs.exe", target_dir)
            zf.extract("lakectl.exe", target_dir)
    else:
        with tarfile.open(fileobj=content, mode="r:gz") as tf:
            tf.extract("lakefs", target_dir, filter="data")
            tf.extract("lakectl", target_dir, filter="data")
            for name in ("lakefs", "lakectl"):
                p = os.path.join(target_dir, name)
                os.chmod(p, os.stat(p).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def read_pyproject_version() -> str:
    """Read the package version from pyproject.toml."""
    pyproject = Path(__file__).parent / "pyproject.toml"
    for line in pyproject.read_text().splitlines():
        if line.strip().startswith("version"):
            # version = "0.1.0"
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("Could not find version in pyproject.toml")


def build_wheel(version: str, pkg_version: str, plat: dict, dist_dir: Path):
    """Build a single platform wheel.

    The wheel is a zip archive with the following structure:
        lakefs_cli/__init__.py
        lakefs_cli/bin/lakefs[.exe]
        lakefs_cli/bin/lakectl[.exe]
        lakefs_cli-{pkg_version}.dist-info/METADATA
        lakefs_cli-{pkg_version}.dist-info/WHEEL
        lakefs_cli-{pkg_version}.dist-info/RECORD
    """
    tag = f"py3-none-{plat['wheel_tag']}"
    wheel_name = f"lakefs_cli-{pkg_version}-{tag}.whl"

    url = (
        f"{DOWNLOAD_URL}v{version}/"
        f"lakeFS_{version}_{plat['os']}_{plat['arch']}.{plat['compression']}"
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # Download and extract binaries
        content = download_file(url)
        bin_dir = tmp_path / "lakefs_cli" / "bin"
        bin_dir.mkdir(parents=True)
        extract_binaries(content, str(bin_dir), plat)

        # Copy __init__.py
        pkg_init = Path(__file__).parent / "lakefs_cli" / "__init__.py"
        init_dest = tmp_path / "lakefs_cli" / "__init__.py"
        shutil.copy2(pkg_init, init_dest)

        # Create dist-info
        dist_info = tmp_path / f"lakefs_cli-{pkg_version}.dist-info"
        dist_info.mkdir()

        (dist_info / "METADATA").write_text(
            f"Metadata-Version: 2.1\n"
            f"Name: lakefs-cli\n"
            f"Version: {pkg_version}\n"
            f"Summary: Companion package shipping pre-built lakeFS and lakectl binaries\n"
            f"Author-email: Treeverse <services@treeverse.io>\n"
            f"License: Apache-2.0\n"
            f"Requires-Python: >=3.10\n"
        )

        (dist_info / "WHEEL").write_text(
            f"Wheel-Version: 1.0\n"
            f"Generator: lakefs-cli-build-wheels\n"
            f"Root-Is-Purelib: false\n"
            f"Tag: {tag}\n"
        )

        # Build RECORD (without its own hash, per spec)
        record_lines = []
        for root, _dirs, files in os.walk(tmp):
            for fname in files:
                fpath = Path(root) / fname
                rel = fpath.relative_to(tmp_path)
                record_lines.append(f"{rel},,")
        record_lines.append(f"{dist_info.name}/RECORD,,")
        (dist_info / "RECORD").write_text("\n".join(record_lines) + "\n")

        # Create the wheel (it's just a zip)
        dist_dir.mkdir(parents=True, exist_ok=True)
        wheel_path = dist_dir / wheel_name
        with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as whl:
            for root, _dirs, files in os.walk(tmp):
                for fname in files:
                    fpath = Path(root) / fname
                    arcname = str(fpath.relative_to(tmp_path))
                    whl.write(fpath, arcname)

    print(f"  Built {wheel_path}")
    return wheel_path


def main():
    parser = argparse.ArgumentParser(description="Build lakefs-cli platform wheels")
    parser.add_argument("version", help="lakeFS release version (e.g. 1.50.0)")
    parser.add_argument(
        "--pkg-version",
        default=None,
        help="Python package version (default: read from pyproject.toml)",
    )
    parser.add_argument(
        "--dist-dir",
        default="dist",
        help="Output directory for wheels (default: dist)",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Build only for this platform tag (e.g. macosx_11_0_arm64)",
    )
    args = parser.parse_args()

    pkg_version = args.pkg_version or read_pyproject_version()
    dist_dir = Path(args.dist_dir)

    platforms = PLATFORMS
    if args.platform:
        platforms = [p for p in PLATFORMS if p["wheel_tag"] == args.platform]
        if not platforms:
            valid = ", ".join(p["wheel_tag"] for p in PLATFORMS)
            print(f"Unknown platform: {args.platform}. Valid options: {valid}", file=sys.stderr)
            sys.exit(1)

    print(f"Building lakefs-cli wheels for lakeFS v{args.version} (pkg {pkg_version})")
    wheels = []
    for plat in platforms:
        print(f"\nPlatform: {plat['wheel_tag']}")
        whl = build_wheel(args.version, pkg_version, plat, dist_dir)
        wheels.append(whl)

    print(f"\nDone! Built {len(wheels)} wheel(s) in {dist_dir}/")


if __name__ == "__main__":
    main()
