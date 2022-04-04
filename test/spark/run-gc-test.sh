#!/bin/bash -au

set -o pipefail

run_lakectl() {
  docker-compose exec -T lakefs lakectl "$@"
}

run_gc () {
  docker-compose run -v ${CLIENT_JAR}:/client/client.jar -T --no-deps --rm spark-submit bash -c 'spark-submit --master spark://spark:7077 --class io.treeverse.clients.GarbageCollector -c spark.hadoop.lakefs.api.url=http://docker.lakefs.io:8000/api/v1 -c spark.hadoop.lakefs.api.access_key=${TESTER_ACCESS_KEY_ID} -c spark.hadoop.lakefs.api.secret_key=${TESTER_SECRET_ACCESS_KEY} -c spark.hadoop.fs.s3a.access.key=${LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID} -c spark.hadoop.fs.s3a.secret.key=${LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY} /client/client.jar $1 us-east-1'
}

clean_repo() {
  local repo=$1
  local branch_names=$(run_lakectl branch list lakefs://${repo} | awk '{print $1}')
  local branch
  for branch in ${branch_names}
  do
    if [[ "${branch}" != "main" ]]; then
      run_lakectl branch delete "lakefs://${repo}/${branch}" -y
    fi
  done
}

initialize_env() {
  local repo=$1
  run_lakectl branch create "lakefs://${repo}/a" -s "lakefs://${repo}/main"
  run_lakectl fs upload "lakefs://${repo}/a/file" -s gc-tests/sample_file
  run_lakectl commit "lakefs://${repo}/a" -m "uploaded file" --epoch-time-seconds 0
  run_lakectl branch create "lakefs://${repo}/b" -s "lakefs://${repo}/a"
}

delete_and_commit() {
  local test_case=$1
  local repo=$2
  echo ${test_case} | jq -c '.branches []' | while read branch_props; do
    local branch_name=$(echo ${branch_props} | jq -r '.branch_name')
    local days_ago=$(echo ${branch_props} | jq -r '.delete_commit_days_ago')
    if [[ ${days_ago} -gt -1 ]]
    then
      run_lakectl fs rm "lakefs://${repo}/${branch_name}/file"
      epoch_commit_date_in_seconds=$(( ${current_epoch_in_seconds} - ${day_in_seconds} * ${days_ago} ))
      run_lakectl commit "lakefs://${repo}/${branch_name}" --allow-empty-message --epoch-time-seconds ${epoch_commit_date_in_seconds}
    else   # This means that the branch should be deleted
      run_lakectl branch delete "lakefs://${repo}/${branch_name}" -y
    fi
  done
}

last_commit_ref() {
  local repo=$1
  local branch=$2
  run_lakectl log lakefs://${repo}/${branch} --amount 1 | grep "ID: " | awk '{ print $2 }'
}

validate_gc_job() {
  local test_case=$1
  local repo=$2
  local existing_ref=$3
  local file_should_be_deleted=$(echo ${test_case} | jq -r '.file_deleted')
  if run_lakectl fs cat lakefs://${repo}/${existing_ref}/file > /dev/null 2>&1 && ${file_should_be_deleted} ; then
    echo "Expected the file to be removed by the garbage collector but it has remained in the repository"
    return 1
  elif ! run_lakectl fs cat lakefs://${repo}/${existing_ref}/file > /dev/null && ! ${file_should_be_deleted} ; then
    echo "Expected the file to remain in the repository but it was removed by the garbage collector"
    return 1
  fi
  echo ${test_case} | jq -c '.branches []' | while read branch_props; do
    local days_ago=$(echo ${branch_props} | jq -r '.delete_commit_days_ago')
    if [[ ${days_ago} -gt -1 ]]; then
      local branch_name=$(echo ${branch_props} | jq -r '.branch_name')
      for location in \
        lakefs://${repo}/${branch_name}/not_deleted_file1 \
        lakefs://${repo}/${branch_name}/not_deleted_file2 \
        lakefs://${repo}/${branch_name}/not_deleted_file3
      do
        if ! run_lakectl fs cat ${location} > /dev/null; then
          echo "expected ${location} to exist"
          return 1
        fi
      done
    fi
  done
}

clean_main_branch() {
  local repo=$1
  run_lakectl fs rm "lakefs://${repo}/main/not_deleted_file1"
  run_lakectl fs rm "lakefs://${repo}/main/not_deleted_file2"
  run_lakectl fs rm "lakefs://${repo}/main/not_deleted_file3"
  run_lakectl commit "lakefs://${repo}/main" -m "delete the undeleted files"
}

#################################
######## Tests Execution ########
#################################

day_in_seconds=86400
current_epoch_in_seconds=$(date +%s)

run_lakectl fs upload "lakefs://${REPOSITORY}/main/not_deleted_file1" -s gc-tests/sample_file
run_lakectl fs upload "lakefs://${REPOSITORY}/main/not_deleted_file2" -s gc-tests/sample_file
run_lakectl fs upload "lakefs://${REPOSITORY}/main/not_deleted_file3" -s gc-tests/sample_file
run_lakectl commit "lakefs://${REPOSITORY}/main" -m "add three files not to be deleted" --epoch-time-seconds 0

failed_tests=()
while read test_case; do
  test_description=$(echo "${test_case}" | jq -r '.description')
  echo "Test: ${test_description}"
  initialize_env ${REPOSITORY}
  file_existing_ref=$(last_commit_ref ${REPOSITORY} a)
  echo "${test_case}" | jq --raw-output '.policy' | run_lakectl gc set-config lakefs://${REPOSITORY} -f -
  delete_and_commit "${test_case}" ${REPOSITORY}
  run_gc ${REPOSITORY}
  if ! validate_gc_job "${test_case}" ${REPOSITORY} ${file_existing_ref}; then
    failed_tests+=("${test_description}")
  fi
  clean_repo ${REPOSITORY}
done < <(jq -c '.[]' gc-tests/test_scenarios.json)

clean_main_branch ${REPOSITORY}

if (( ${#failed_tests[@]} > 0)); then
  for ft in "${failed_tests[@]}"; do
      echo "Test: ${ft} - FAILED"
  done
  exit 1
fi

echo "Tests completed successfully"