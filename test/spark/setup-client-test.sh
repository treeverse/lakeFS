#!/bin/bash -ex
#
# setup-client-test.sh - set up lakeFS to run Spark app tests.  Run once per lakeFS.

docker-compose exec -T lakefs /app/wait-for localhost:8000

docker-compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}'


docker-compose exec -T lakefs lakectl repo create-bare lakefs://test-data $STORAGE_NAMESPACE
docker-compose exec -T lakefs lakectl refs-restore lakefs://test-data --manifest /local/refs.json
