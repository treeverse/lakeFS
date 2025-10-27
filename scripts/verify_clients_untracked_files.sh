#!/bin/bash

set -euo pipefail

echo "Checking for untracked files in 'clients/'..."

if git status --porcelain clients/ | grep -E '^\?\?'; then
    cat << EOF
Error: Untracked files found in the 'clients/' directory.

The 'clients/' directory contains auto-generated code that must be tracked in Git. Untracked files suggest new files were created and need to be added.

To resolve this:

1. Remove the 'clients/' directory:
   rm -fr clients/

2. Restore 'clients/' to the last commit:
   git restore -- clients/

3. Regenerate files in 'clients/':
   make gen

4. Stage the regenerated files:
   git add clients/

5. Commit the changes:
   git commit -m 'Regenerate clients/ files'
EOF
    exit 1
else
    echo "No untracked files found." 
    exit 0
fi
