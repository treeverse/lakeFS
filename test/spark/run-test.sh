#!/bin/sh -e
#
# run-test.sh - submit spark app after setup lakefs and upload dataset.
#
# uses the following environment variables:
#   STORAGE_NAMESPACE - storage namespace used for repository creation
# 
# NOTE that this script should be run from the root project in order for docker compose to volume mount the project

STORAGE_NAMESPACE=${STORAGE_NAMESPACE:-local://}

echo "==> wait for lakefs"
docker-compose exec -T lakefs /app/wait-for localhost:8000

echo "==> setup lakefs with known credentials"
docker-compose exec -T lakefs sh -c 'lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}'

echo "==> create repository"
docker-compose exec -T lakefs lakectl repo create lakefs://example ${STORAGE_NAMESPACE} -d main

echo "==> upload dataset"
docker-compose exec -T lakefs lakectl fs upload -s /local/app/data-sets/sonnets.txt lakefs://example/main/sonnets.txt

echo "==> submit spark app"
docker-compose run -T --no-deps --rm spark-submit sh -c 'spark-submit --master spark://spark:7077 -c "spark.hadoop.fs.s3a.access.key=${TESTER_ACCESS_KEY_ID}" -c "spark.hadoop.fs.s3a.secret.key=${TESTER_SECRET_ACCESS_KEY}" --class Sonnets /local/app/target/scala-2.12/sonnets_2.12-0.1.jar'
