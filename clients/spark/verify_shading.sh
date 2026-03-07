#!/bin/bash
# Standalone shading verification script.
# Usage: ./verify_shading.sh <assembly-jar-path>
#
# Checks that Google classes (com/google/api/, com/google/cloud/, etc.) have been
# properly relocated by the shade plugin. If any .class files remain at their
# original paths, the jar will break on environments like Dataproc where the
# same classes exist on Spark's classpath.
#
# See: https://github.com/treeverse/lakeFS/issues/10136

set -euo pipefail

JAR="${1:?Usage: $0 <assembly-jar-path>}"

MUST_BE_SHADED=(
  "com/google/api/"
  "com/google/cloud/storage/"
  "com/google/type/"
  "com/google/rpc/"
  "com/google/longrunning/"
  "com/google/iam/"
  "com/google/logging/"
  "com/google/protobuf/"
)
# NOTE: com/google/cloud/hadoop/ is intentionally NOT shaded — Spark loads
# the GCS connector by class name (fs.gs.impl=...GoogleHadoopFileSystem).

# Build grep pattern
PATTERN=$(printf "%s\n" "${MUST_BE_SHADED[@]}" | sed 's/\//\\\//g' | paste -sd'|' -)

UNSHADED=$(jar tf "$JAR" | grep '\.class$' | grep -E "^(${PATTERN})" || true)

if [ -n "$UNSHADED" ]; then
  COUNT=$(echo "$UNSHADED" | wc -l)
  echo "ERROR: Found $COUNT unshaded Google classes in assembly jar:"
  echo "$UNSHADED" | head -20 | sed 's/^/  /'
  if [ "$COUNT" -gt 20 ]; then
    echo "  ... and $((COUNT - 20)) more"
  fi
  echo ""
  echo "FAILED: Add the missing packages to assemblyShadeRules in build.sbt."
  echo "See: https://github.com/treeverse/lakeFS/issues/10136"
  exit 1
else
  echo "PASSED: All checked Google packages are properly relocated."
  exit 0
fi
