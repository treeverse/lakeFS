#!/bin/bash -ex
REPOSITORY=${REPOSITORY:-example}


docker-compose exec -T lakefs lakectl repo create "lakefs://${REPOSITORY}" ${STORAGE_NAMESPACE} -d main

docker-compose exec -T lakefs lakectl fs upload -s /local/app/data-sets/sonnets.txt "lakefs://${REPOSITORY}/main/sonnets.txt"

mvn test
