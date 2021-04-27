#!/bin/sh -e
#
# submit-spark-app - submit spark app (nessie/spark-app) after setup lakefs and upload dataset.
#
# uses the following environment variables:
#   TESTER_ACCESS_KEY_ID / TESTER_SECRET_ACCESS_KEY - key/secret used for lakefs setup/access
#   NESSIE_STORAGE_NAMESPACE - storange namespace used for repository creation
# 
# NOTE that this script should be run from the root project in order for docker compose to volume mount the project

echo "cleanup previous env"
docker-compose down -v || true

echo "start env"
docker-compose up -d

echo "wait for lakefs"
docker-compose exec -T lakefs /app/wait-for localhost:8000

echo "setup lakefs with known credentials"
docker-compose exec -T lakefs lakefs setup --user-name tester --access-key-id ${TESTER_ACCESS_KEY_ID} --secret-access-key ${TESTER_SECRET_ACCESS_KEY}

echo "create repository"
docker-compose exec -T lakefs lakectl --config /lakefs/nessie/ops/lakectl-tester.yaml repo create lakefs://example ${NESSIE_STORAGE_NAMESPACE} -d master

echo "upload dataset"
docker-compose exec -T lakefs lakectl --config /lakefs/nessie/ops/lakectl-tester.yaml fs upload -s /lakefs/nessie/spark-app/data-sets/sonnets.txt lakefs://example/master/sonnets.txt

echo "submit spark app"
docker-compose run -T --no-deps --rm spark-submit spark-submit --master spark://spark:7077 -c "spark.hadoop.fs.s3a.access.key=${TESTER_ACCESS_KEY_ID}" -c "spark.hadoop.fs.s3a.secret.key=${TESTER_SECRET_ACCESS_KEY}" --class Sonnets /lakefs/nessie/spark-app/target/scala-2.12/sonnets_2.12-0.1.jar
