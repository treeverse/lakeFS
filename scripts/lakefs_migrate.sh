#!/bin/bash
if [ $# -lt 2 ]; then
    echo "schema and command(s) are required!"
    exit 1
fi
PG_BASE_URL=${PG_BASE_URL:=postgres://localhost:5432/postgres}
schema=$1
shift
migrate -database "${PG_BASE_URL}?search_path=${schema}&sslmode=disable" -path $(dirname $0)/../ddl/${schema} $*

