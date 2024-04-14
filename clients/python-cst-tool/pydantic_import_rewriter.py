#!/usr/bin/env python3
import pathlib
import sys
import fnmatch
import re
from pathlib import Path

import click
import libcst as cst
import libcst.matchers as m

# Taken from https://github.com/pydantic/bump-pydantic
DEFAULT_IGNORES = [".venv/**", ".tox/**"]
MATCH_SEP = r"(?:/|\\)"
MATCH_SEP_OR_END = r"(?:/|\\|\Z)"
MATCH_NON_RECURSIVE = r"[^/\\]*"
MATCH_RECURSIVE = r"(?:.*)"


# Taken from https://github.com/pydantic/bump-pydantic
def glob_to_re(pattern: str) -> str:
    """Translate a glob pattern to a regular expression for matching."""
    fragments = []
    for segment in re.split(r"/|\\", pattern):
        if segment == "":
            continue
        if segment == "**":
            # Remove previous separator match, so the recursive match can match zero or more segments.
            if fragments and fragments[-1] == MATCH_SEP:
                fragments.pop()
            fragments.append(MATCH_RECURSIVE)
        elif "**" in segment:
            raise ValueError("invalid pattern: '**' can only be an entire path component")
        else:
            fragment = fnmatch.translate(segment)
            fragment = fragment.replace(r"(?s:", r"(?:")
            fragment = fragment.replace(r".*", MATCH_NON_RECURSIVE)
            fragment = fragment.replace(r"\Z", r"")
            fragments.append(fragment)
        fragments.append(MATCH_SEP)
    # Remove trailing MATCH_SEP, so it can be replaced with MATCH_SEP_OR_END.
    if fragments and fragments[-1] == MATCH_SEP:
        fragments.pop()
    fragments.append(MATCH_SEP_OR_END)
    return rf"(?s:{''.join(fragments)})"


# Taken from https://github.com/pydantic/bump-pydantic
def match_glob(path: Path, pattern: str) -> bool:
    """Check if a path matches a glob pattern.

    If the pattern ends with a directory separator, the path must be a directory.
    """
    match = bool(re.fullmatch(glob_to_re(pattern), str(path)))
    if pattern.endswith("/") or pattern.endswith("\\"):
        return match and path.is_dir()
    return match


def wrap_import(import_from: cst.ImportFrom) -> cst.Try:
    return cst.Try(
        body=cst.IndentedBlock(
            body=[cst.SimpleStatementLine(
                body=[cst.ImportFrom(
                    module=cst.Attribute(value=cst.Name(value='pydantic'), attr=cst.Name(value='v1')),
                    names=import_from.names,
                )],
            )],
        ),
        handlers=[
            cst.ExceptHandler(
                type=cst.Name(value='ImportError'),
                body=cst.IndentedBlock(
                    body=[cst.SimpleStatementLine(body=[import_from])],
                ),
            ),
        ],
    )


class ImportFixer(cst.CSTTransformer):

    def leave_SimpleStatementLine(self, orignal_node, updated_node):
        if m.matches(updated_node.body[0], m.ImportFrom()):
            import_from: cst.ImportFrom = updated_node.body[0]
            if m.matches(import_from, m.ImportFrom(module=m.Name("pydantic"))):
                return wrap_import(import_from)
        return updated_node


@click.command()
@click.option('path', '--path', required=True, type=click.Path())
def main(path: str):
    path = pathlib.Path(path)
    all_files = sorted(path.glob("**/*.py"))

    filtered_files = [file for file in all_files
                      if not any(match_glob(file, pattern)
                                 for pattern in DEFAULT_IGNORES)]
    files = [str(file.relative_to(".")) for file in filtered_files]
    if not files:
        print("No files to process.")
        sys.exit(1)

    print(f"Found {len(files)} files to process.")

    for f in files:
        print(f'file: {f}')
        with open(f, 'rb') as fh:
            source = fh.read()
        mod = cst.parse_module(source)
        transformer = ImportFixer()
        mod = mod.visit(transformer)

        with open(f, 'w') as fh:
            fh.write(mod.code)


if __name__ == '__main__':
    main()
