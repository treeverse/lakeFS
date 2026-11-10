#!/usr/bin/env bash
set -euo pipefail

storage_namespace=""
repository=""
sonnet_jar=""
client_version=""
aws_access_key=""
aws_secret_key=""
redirect=false
access_mode="s3_gateway"
region="None"

usage() {
    cat <<'EOF'
Usage: run-test.sh --storage_namespace <namespace> --repository <name> --sonnet_jar <jar> [options]

Options:
  --client_version <version>
  --aws_access_key <key>
  --aws_secret_key <secret>
  --redirect
  --access_mode <s3_gateway|hadoopfs|hadoopfs_presigned>
  --region <region>
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --storage_namespace)
            storage_namespace="$2"
            shift 2
            ;;
        --repository)
            repository="$2"
            shift 2
            ;;
        --sonnet_jar)
            sonnet_jar="$2"
            shift 2
            ;;
        --client_version)
            client_version="$2"
            shift 2
            ;;
        --aws_access_key)
            aws_access_key="$2"
            shift 2
            ;;
        --aws_secret_key)
            aws_secret_key="$2"
            shift 2
            ;;
        --redirect)
            redirect=true
            shift
            ;;
        --access_mode)
            access_mode="$2"
            shift 2
            ;;
        --region)
            region="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "$storage_namespace" || -z "$repository" || -z "$sonnet_jar" ]]; then
    usage >&2
    exit 2
fi

case "$access_mode" in
    s3_gateway|hadoopfs|hadoopfs_presigned) ;;
    *)
        echo "Invalid access mode: $access_mode" >&2
        exit 2
        ;;
esac

lakefs_access_key='AKIAIOSFODNN7EXAMPLE'
lakefs_secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

for attempt in {1..7}; do
    if docker compose exec -T lakefs lakectl repo list >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" -eq 7 ]]; then
        echo "lakeFS did not become ready after 7 attempts" >&2
        exit 1
    fi
    sleep 1
done

docker compose exec -T lakefs lakectl repo create "lakefs://${repository}" "$storage_namespace"
docker compose exec -T lakefs lakectl fs upload \
    --pre-sign=false \
    -s /local/app/data-sets/sonnets.txt \
    "lakefs://${repository}/main/sonnets.txt"

submit_args=(spark-submit --master spark://spark:7077)

if [[ -n "$client_version" ]]; then
    submit_args+=(--packages "io.lakefs:hadoop-lakefs-assembly:${client_version}")
else
    submit_args+=(--jars /target/client.jar)
fi

spark_configs=(
    "spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem"
    "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true"
    "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true"
    "spark.hadoop.fs.lakefs.endpoint=http://lakefs:8000/api/v1"
    "spark.hadoop.fs.lakefs.access.key=${lakefs_access_key}"
    "spark.hadoop.fs.lakefs.secret.key=${lakefs_secret_key}"
)

case "$access_mode" in
    hadoopfs)
        scheme="lakefs"
        spark_configs+=(
            "spark.hadoop.fs.s3a.access.key=${aws_access_key}"
            "spark.hadoop.fs.s3a.secret.key=${aws_secret_key}"
            "spark.hadoop.fs.s3a.region=${region}"
        )
        ;;
    hadoopfs_presigned)
        scheme="lakefs"
        spark_configs+=("spark.hadoop.fs.lakefs.access.mode=presigned")
        ;;
    s3_gateway)
        scheme="s3a"
        spark_configs=(
            "spark.hadoop.fs.s3a.access.key=${lakefs_access_key}"
            "spark.hadoop.fs.s3a.secret.key=${lakefs_secret_key}"
            "spark.hadoop.fs.s3a.endpoint=s3.docker.lakefs.io:8000"
            "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
        )
        if [[ "$redirect" == true ]]; then
            spark_configs+=("spark.hadoop.fs.s3a.path.style.access=true")
            spark_configs+=("spark.hadoop.fs.s3a.user.agent.prefix=s3RedirectionSupport")
            if [[ "${SPARK_TAG:-}" != 4* ]]; then
                spark_configs+=("spark.hadoop.fs.s3a.signing-algorithm=QueryStringSignerType")
            fi
        fi
        ;;
esac

for config in "${spark_configs[@]}"; do
    submit_args+=(-c "$config")
done

submit_args+=(
    --class Sonnets
    "/local/app/target/${sonnet_jar}"
    "${scheme}://${repository}/main/sonnets.txt"
    "${scheme}://${repository}/main/sonnets-wordcount"
)

docker compose run --no-deps --rm -T spark-submit "${submit_args[@]}"
