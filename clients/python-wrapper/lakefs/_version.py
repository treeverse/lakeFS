"""A utility to parse the lakefs version for the Makefile."""

import sys
from pathlib import Path

if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib


def parse() -> str:
    """Returns the lakefs version as a string directly from pyproject.toml."""
    pyproject_path = Path(__file__).parents[1] / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        metadata = tomllib.load(f)
        return metadata["project"]["version"]


if __name__ == "__main__":
    print(parse())
