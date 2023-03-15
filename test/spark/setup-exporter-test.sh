#!/bin/bash -ex
#
# setup-exporter-test.sh - set up lakeFS to run Spark exporter tests.

docker-compose exec -T lakefs /app/wait-for localhost:8000

docker-compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}'
docker-compose exec -T lakefs lakectl repo create-bare lakefs://${REPOSITORY//./-} ${STORAGE_NAMESPACE}
docker-compose exec -T lakefs lakectl refs-restore lakefs://${REPOSITORY//./-} --manifest /local/refs.json
