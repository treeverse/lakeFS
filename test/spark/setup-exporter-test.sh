#!/bin/bash -ex
#
# setup-exporter-test.sh - set up lakeFS to run Spark exporter tests.

docker-compose exec -T lakefs /app/wait-for localhost:8000

docker-compose exec -T lakefs lakectl repo create-bare lakefs://${REPOSITORY//./-} ${STORAGE_NAMESPACE}
docker-compose exec -T lakefs lakectl refs-restore lakefs://${REPOSITORY//./-} --manifest /local/refs.json
