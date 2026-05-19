#!/usr/bin/env bash
#
# Extract unique https://docs.lakefs.io/* URLs referenced from the webui source.
# Prints one URL per line on stdout. Intended for piping into a link checker.
#
# Usage:
#   .github/scripts/extract-docs-links.sh [webui-src-dir]
#
# Defaults to <repo-root>/webui/src when no argument is given.

set -euo pipefail

SRC_DIR="${1:-webui/src}"

if [ ! -d "$SRC_DIR" ]; then
  echo "extract-docs-links: directory not found: $SRC_DIR" >&2
  exit 1
fi

# Match https://docs.lakefs.io followed by any URL-safe chars, stopping at the
# usual JSX/HTML/JS string delimiters and whitespace.
grep -rEhoI \
  --include='*.ts' --include='*.tsx' \
  --include='*.js' --include='*.jsx' \
  --include='*.html' \
  'https://docs\.lakefs\.io[^"'"'"'`<> )]*' "$SRC_DIR" \
  | sort -u
