#!/bin/bash

cd "$(dirname "$0")" || exit
. set_env_vars.sh

LAKEFS_LOG=$(mktemp --suffix=.log --tmpdir lakefs_XXX)
TEST_LOG=$(mktemp --suffix=.log --tmpdir lakefs_tests_XXX)
RUN_RESULT=2

trap cleanup EXIT

cleanup() {
  pkill lakefs
  if [ $RUN_RESULT == 0 ]; then
    echo "Tests successful, cleaning up logs files"
    rm $LAKEFS_LOG
    rm $TEST_LOG
  elif [ $RUN_RESULT == 1 ]; then
    echo "Tests failed! See logs for more information: $LAKEFS_LOG $TEST_LOG"
  fi
}

invalid_option() {
  echo "Error: Invalid option"
  Help
}

help() {
  echo "Local system tests execution"
  echo
  echo "Syntax: runner [-h|r]"
  echo "options:"
  echo "h     Print this Help."
  echo "r     Runs the given process [lakefs | tests | all]."
  echo
}

wait_for_lakefs_ready() {
  echo "Waiting for lakeFS ready"
  until curl --output /dev/null --silent --head --fail localhost:8000/_health; do
      printf '.'
      sleep 1
  done
  echo "lakeFS is ready"
}

run_tests() {
  echo "Run Tests (logs at $TEST_LOG)"
  go test -v ../../esti --args --system-tests --use-local-credentials --skip=".*GC" "$@" | tee "$TEST_LOG"
  return "${PIPESTATUS[0]}"
}

run_lakefs() {
  echo "Run LakeFS (logs at $LAKEFS_LOG)"
  lakefs run -c lakefs.yaml | tee "$LAKEFS_LOG"
}

run_all() {
  run_lakefs &

  wait_for_lakefs_ready

  run_tests "$@"
  RUN_RESULT=$?
  return $RUN_RESULT		# restore failure (the previous line succeeds in sh)
}

# Get the options
while getopts ":hr:" option; do
  case $option in
  h) # Display Help
    help
    exit
    ;;
  r) # Run
    run=$OPTARG
    shift 2
    if [ "$run" == "test" ]; then
      run_tests "$@"
    elif [ "$run" == "lakefs" ]; then
      run_lakefs
    elif [ "$run" == "all" ]; then
      run_all "$@"
    else
      invalid_option
    fi
    exit
    ;;
  \?) # Invalid option
    invalid_option
    exit
    ;;
  esac
done

help # No arguments passed
