#!/bin/bash

MAX_ATTEMPTS=15

# Support both bitnami and treeverse spark paths
SPARK_BASE=${SPARK_BASE:-bitnami}  # Default to bitnami for backward compatibility
if [ "$SPARK_BASE" = "treeverse" ]; then
    SPARK_PATH="/opt/treeverse/spark"
else
    SPARK_PATH="/opt/bitnami/spark"
fi

attempt_counter=0
while ! ${SPARK_PATH}/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2; do
  if [ ${attempt_counter} -eq ${MAX_ATTEMPTS} ]; then
    echo "Operation got to max attempts"
    exit 1
  fi

  attempt_counter=$(($attempt_counter + 1))
  echo "Retry run Hive Thrift Server: ${attempt_counter}/${MAX_ATTEMPTS}"
  sleep 1
done
