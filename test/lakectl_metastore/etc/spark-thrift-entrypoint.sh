#!/bin/bash

MAX_ATTEMPTS=15

attempt_counter=0
while ! /opt/bitnami/spark/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2; do
  if [ ${attempt_counter} -eq ${MAX_ATTEMPTS} ]; then
    echo "Operation got to max attempts"
    exit 1
  fi

  attempt_counter=$(($attempt_counter + 1))
  echo "Retry run Hive Thrift Server: ${attempt_counter}/${MAX_ATTEMPTS}"
  sleep 1
done
