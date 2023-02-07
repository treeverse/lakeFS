#!/bin/bash -ex
#
# run-test.sh - submit spark app after setup lakefs and upload dataset.
#
# uses the following environment variables:
#   REPOSITORY - name of repository to create and use in test
#   STORAGE_NAMESPACE - storage namespace used for repository creation
#   USE_DIRECT_ACCESS - if set (to *anything*), use the direct thick Spark client
#   SONNET_JAR - Sonnet jar to submit, relative to build target directory
# 
# NOTE that this script should be run from the root project in order for docker compose to volume mount the project

./fail

STORAGE_NAMESPACE=${STORAGE_NAMESPACE:-local://}
REPOSITORY=${REPOSITORY:-example}
SONNET_JAR=${SONNET_JAR:-sonnets-246/target/sonnets/246/scala-2.12/sonnets-246_2.12-0.1.jar}

if [ -n "$CLIENT_VERSION" ]; then # Using a published or a built version
    CLIENT_CMD="--packages io.lakefs:hadoop-lakefs-assembly:${CLIENT_VERSION}"
else
    CLIENT_CMD="--jars /target/client.jar"
fi

export input="lakefs://${REPOSITORY}/main/sonnets.txt"
export output="lakefs://${REPOSITORY}/main/sonnets-wordcount"

docker-compose exec -T lakefs lakectl repo create "lakefs://${REPOSITORY}" ${STORAGE_NAMESPACE} -d main

docker-compose exec -T lakefs lakectl fs upload -s /local/app/data-sets/sonnets.txt "lakefs://${REPOSITORY}/main/sonnets.txt"

if [ -v USE_DIRECT_ACCESS ]; then # Direct access using thick Spark client.
    docker-compose run -v $PWD/../../clients/hadoopfs/target/:/target/ -T --no-deps --rm spark-submit sh -c 'spark-submit --master spark://spark:7077 '"${CLIENT_CMD}"' -c "spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem" --conf=spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf=spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  -c spark.hadoop.fs.lakefs.endpoint=http://lakefs:8000/api/v1 -c "spark.hadoop.fs.lakefs.access.key=${TESTER_ACCESS_KEY_ID}" -c "spark.hadoop.fs.lakefs.secret.key=${TESTER_SECRET_ACCESS_KEY}" -c "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" -c "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" -c "spark.hadoop.fs.s3a.region=${AWS_REGION}" --class Sonnets /local/app/target/'${SONNET_JAR}' ${input} ${output}'
else				# Access via S3 gateway using regular Spark client.
    export s3input="s3a://${REPOSITORY}/main/sonnets.txt"
    export s3output="s3a://${REPOSITORY}/main/sonnets-wordcount"
    docker-compose run -T --no-deps --rm spark-submit sh -c 'spark-submit --master spark://spark:7077 -c "spark.hadoop.fs.s3a.access.key=${TESTER_ACCESS_KEY_ID}" -c "spark.hadoop.fs.s3a.secret.key=${TESTER_SECRET_ACCESS_KEY}" -c spark.hadoop.fs.s3a.endpoint=s3.docker.lakefs.io:8000 -c spark.hadoop.fs.s3a.connection.ssl.enabled=false --class Sonnets /local/app/target/'${SONNET_JAR}' ${s3input} ${s3output}'
fi
