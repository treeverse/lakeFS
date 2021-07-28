#!/bin/bash -ex
REPOSITORY=${REPOSITORY:-example}

docker-compose exec -T lakefs lakectl repo delete -y "lakefs://${REPOSITORY}" || echo "no need to delete"
docker-compose exec -T lakefs lakectl repo create "lakefs://${REPOSITORY}" ${STORAGE_NAMESPACE} -d main

cd ../../clients/hadoopfs
mvn test
