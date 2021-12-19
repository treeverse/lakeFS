#!/bin/bash

MAX_ATTEMPTS=60

attempt_counter=0
while :
do
 if [ ${attempt_counter} -eq ${MAX_ATTEMPTS} ]; then
    echo "Operation got to max attempts"
    exit 1
  fi

  attempt_counter=$(($attempt_counter + 1))
  echo "Retry run Hive Thrift Server: ${attempt_counter}/${MAX_ATTEMPTS}"
  starting=$(curl http://trino:8080/v1/info | jq .starting)

  if [ "$starting" = "false" ] ; then
    exit 0
  fi
  sleep 1
done
echo "connect to trino reached timeout"
exit 1
