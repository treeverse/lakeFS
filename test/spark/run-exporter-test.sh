#!/bin/bash -aux

set -o pipefail

REPOSITORY=${REPOSITORY//./-}
# Run Export
docker compose run -v  ${CLIENT_JAR}:/client/client.jar -T --no-deps --rm spark-submit bash -c "spark-submit  --master spark://spark:7077 --conf spark.hadoop.lakefs.api.url=http:/docker.lakefs.io:8000/api/v1   --conf spark.hadoop.lakefs.api.access_key=\${TESTER_ACCESS_KEY_ID}   --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false   --conf spark.hadoop.lakefs.api.secret_key=\${TESTER_SECRET_ACCESS_KEY}   --class io.treeverse.clients.Main  /client/client.jar ${REPOSITORY} ${EXPORT_LOCATION}   --branch=main"

# Validate export
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

docker compose exec -T lakefs lakectl fs ls --recursive --no-color "lakefs://${REPOSITORY}/main/" | awk '{print $8}' | sort > ${lakectl_out}

export_bucket=`echo $EXPORT_LOCATION | sed -E 's!^s3://([^/]*)/.*!\1!'`
export_key=`echo $EXPORT_LOCATION | sed -E 's!^s3://[^/]*/!!'`

aws s3api list-objects-v2 \
        --bucket "$export_bucket" --prefix "$export_key" \
	--query "Contents[].[Key]" --output text | \
    sed "s%^${export_key}/%%" | \
    fgrep -v EXPORT_ | \
    sort > ${s3_out}

if ! diff ${lakectl_out} ${s3_out}; then
  echo "The export's location and lakeFS should contain same objects"
  exit 1
fi
