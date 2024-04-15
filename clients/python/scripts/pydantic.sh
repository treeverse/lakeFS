#!/bin/bash -eu

set -o pipefail

filename=$1

while IFS= read -r line; do
    if [ -z "${line}" ]; then
      echo "Nothing to do"
      continue 
    fi
    echo $line
    modules=$(echo $line | awk -F 'from pydantic import ' '{print $2}')
    newline="try:\n    from pydantic.v1 import $modules\nexcept ImportError:\n    from pydantic import $modules"
    
    echo "$newline"
    
    echo "Replacing import statement"
    sed -i "s/$line/$newline/g" "$filename"
done <<< "$(sed -n '/^from pydantic import/p' $filename)"
