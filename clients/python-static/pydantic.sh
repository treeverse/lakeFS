#!/bin/bash -eu

set -o pipefail

filename=$1

if grep -q '^from pydantic import' "$filename"; then
    echo "Processing pydantic imports in $filename"
    awk '/^from pydantic import/ {
        modules = substr($0, 21)
        print "try:"
        print "    from pydantic.v1 import" modules
        print "except ImportError:"
        print "    " $0
        next
    } {print}' "$filename" > "$filename.tmp" && mv "$filename.tmp" "$filename"
else
    echo "Nothing to do"
fi
