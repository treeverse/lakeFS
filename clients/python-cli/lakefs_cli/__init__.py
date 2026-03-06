"""
Companion package for lakeFS that ships pre-built lakefs and lakectl binaries.

Install this package to get the binaries without downloading them at runtime.
"""

import os
import sys


def get_binary_path(name: str) -> str | None:
    """Return the absolute path to an embedded binary, or None if not found.

    Args:
        name: Binary name, either "lakefs" or "lakectl".
    """
    bin_dir = os.path.join(os.path.dirname(__file__), "bin")
    suffix = ".exe" if sys.platform == "win32" else ""
    path = os.path.join(bin_dir, name + suffix)
    return path if os.path.isfile(path) else None
