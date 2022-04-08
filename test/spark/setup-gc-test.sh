#!/bin/bash -ex
#
# setup-gc-test.sh - set up lakeFS to run Spark GC tests.

docker-compose exec -T lakefs /app/wait-for localhost:8000


docker-compose exec -T lakefs lakectl repo create lakefs://${REPOSITORY//./-} ${STORAGE_NAMESPACE}