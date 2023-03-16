#!/bin/bash -ex
#
# setup-test.sh - set up lakeFS to run rclone export tests.  Run once per lakeFS.

docker compose exec -T lakefs /app/wait-for localhost:8000

docker compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id AKIAIOSFODNN7EXAMPLE --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

docker compose exec -T lakefs lakectl repo create lakefs://example $STORAGE_NAMESPACE
