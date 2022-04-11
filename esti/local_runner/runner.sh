#!/bin/bash

InvalidOption() {
  echo "Error: Invalid option"
  Help
}

Help() {
  echo "Local system tests execution"
  echo
  echo "Syntax: runner [-h|r]"
  echo "options:"
  echo "h     Print this Help."
  echo "r     Runs the given process [lakefs | tests]."
  echo
}

RunTests() {
  echo "Run Tests (logs at /tmp/tests.log)"
  go test -v ../../esti --args --system-tests --use-local-credentials "$@" | tee /tmp/tests.log
}

RunLakeFS() {
  command="pg_isready -h localhost -p 5433"
  echo $(eval $command)
  echo "Create Postgres DB via docker compose"
  docker-compose -f ../ops/docker-compose.yaml up --force-recreate -V -d postgres

  echo "Waiting for DB ready"
  eval $command

  while [ $? != 0 ]; do
    sleep 1
    eval $command
  done
  echo "DB is ready"

  echo "Run LakeFS (logs at /tmp/lakefs.log)"
  lakefs run -c lakefs.yaml | tee /tmp/lakefs.log

}

# Get the options
while getopts ":hr:" option; do
  shift
  case $option in
  h) # Display Help
    Help
    exit
    ;;
  r) # Run
    run=$OPTARG
    shift
    pushd "$(dirname "$0")" || exit
    . set_env_vars.sh
    if [ "$run" == "test" ]; then
      RunTests "$@"
    elif [ "$run" == "lakefs" ]; then
      RunLakeFS
    else
      InvalidOption
    fi
    popd || exit
    exit
    ;;
  \?) # Invalid option
    InvalidOption
    exit
    ;;
  esac
done

Help # No arguments passed
