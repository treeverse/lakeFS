#!/bin/bash

set -e

# `--porcelain` gives the output in an easy-to-parse format for scripts
git_status_output=$(git status clients/ --porcelain)

untracked_files_found=false

while IFS= read -r line; do
    if [[ $line == \?\?* ]]; then
        untracked_files_found=true
	break
    fi
done <<< "$git_status_output"

if $untracked_files_found; then
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
    exit 0
fi
