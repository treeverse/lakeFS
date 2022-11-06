#!/bin/bash -ex
#
# setup-test.sh - set up lakeFS to run Spark app tests.  Run once per lakeFS.

docker compose exec -T lakefs /app/wait-for localhost:8000

docker compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}'

