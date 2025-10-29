#!/bin/bash -ex

docker compose exec -T lakefs /app/wait-for localhost:8000
docker compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}'
aws s3api create-bucket --endpoint-url http://s3.local.lakefs.io:9000 --bucket test-bucket
docker compose exec -T lakefs lakectl repo create "lakefs://${REPOSITORY}" "${STORAGE_NAMESPACE}" -d main
