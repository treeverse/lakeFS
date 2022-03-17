#!/bin/bash -ex
#
# setup-test.sh - set up lakeFS to run Spark app tests.  Run once per lakeFS.

docker-compose exec -T lakefs /app/wait-for localhost:8000

docker-compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${LAKECTL_CREDENTIALS_ACCESS_KEY_ID} --secret-access-key ${LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY}'

docker-compose exec -T lakefs sh -c 'lakectl repo create lakefs://example ${STORAGE_NAMESPACE} -d main'

docker-compose run --entrypoint '/usr/app/wait-for-trino.sh' dbt

docker-compose exec -T trino sh -c "trino --catalog lakefs --execute \"create schema dbt_main with ( location = 's3://example/main/dbt/' )\" "

