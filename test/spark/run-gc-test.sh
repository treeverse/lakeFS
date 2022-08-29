#!/bin/bash -aux

set -o pipefail

REPOSITORY=${REPOSITORY//./-}

_jq() {
 echo ${1} | base64 --decode | jq -r '.'
}

run_lakectl() {
  echo "lakectl variables: $@"
  docker-compose exec -T lakefs lakectl "$@"
}

run_gc () {
  if [ ${LAKEFS_BLOCKSTORE_TYPE} == "azure" ]; then
    docker-compose run -v ${CLIENT_JAR}:/client/client.jar -T --no-deps --rm spark-submit bash -c "spark-submit -v --packages org.apache.hadoop:hadoop-azure:3.2.1 --master spark://spark:7077 --class io.treeverse.clients.GarbageCollector -c spark.hadoop.lakefs.api.url=http://docker.lakefs.io:8000/api/v1 -c spark.hadoop.lakefs.api.access_key=\${TESTER_ACCESS_KEY_ID} -c spark.hadoop.lakefs.api.secret_key=\${TESTER_SECRET_ACCESS_KEY} -c spark.hadoop.lakefs.api.connection.timeout_seconds=3 -c spark.hadoop.lakefs.api.read.timeout_seconds=8 -c spark.hadoop.fs.azure.account.key.\${LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net=\${LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY} -c spark.sql.shuffle.partitions=7 -c spark.default.parallelism=13 /client/client.jar $1"
  else
    docker-compose run -v ${CLIENT_JAR}:/client/client.jar -T --no-deps --rm spark-submit bash -c "spark-submit -v --master spark://spark:7077 --class io.treeverse.clients.GarbageCollector -c spark.hadoop.lakefs.api.url=http://docker.lakefs.io:8000/api/v1 -c spark.hadoop.lakefs.api.access_key=\${TESTER_ACCESS_KEY_ID} -c spark.hadoop.lakefs.api.secret_key=\${TESTER_SECRET_ACCESS_KEY} -c spark.hadoop.fs.s3a.access.key=\${AWS_ACCESS_KEY_ID} -c spark.hadoop.fs.s3a.secret.key=\${AWS_SECRET_ACCESS_KEY} -c spark.sql.shuffle.partitions=7 -c spark.default.parallelism=13 /client/client.jar $1 us-east-1"
  fi
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

delete_and_commit() {
  local test_case=$1
  local repo=$2
  local test_id=$3
  for branch_props in $(echo ${test_case} | jq -r '.branches [] | @base64'); do
    branch_props=$(_jq ${branch_props})
    local branch_name=$(echo ${branch_props} | jq -r '.branch_name')
    local days_ago=$(echo ${branch_props} | jq -r '.delete_commit_days_ago')
    if [[ ${days_ago} -gt -1 ]]
    then
      run_lakectl fs rm "lakefs://${repo}/${branch_name}/file${test_id}"
      epoch_commit_date_in_seconds=$(( ${current_epoch_in_seconds} - ${day_in_seconds} * ${days_ago} ))
      run_lakectl commit "lakefs://${repo}/${branch_name}" --allow-empty-message --epoch-time-seconds ${epoch_commit_date_in_seconds}
      run_lakectl fs upload "lakefs://${repo}/${branch_name}/file${test_id}not_deleted" -s /local/gc-tests/sample_file
      run_lakectl commit "lakefs://${repo}/${branch_name}" -m "not deleted file commit: ${test_id}" --epoch-time-seconds ${epoch_commit_date_in_seconds} # This is for the previous commit to be the HEAD of the branch outside the retention time (according to GC https://github.com/treeverse/lakeFS/issues/1932)
    else   # This means that the branch should be deleted
      run_lakectl branch delete "lakefs://${repo}/${branch_name}" -y
    fi
  done
}

validate_gc_job() {
  local test_case=$1
  local repo=$2
  local existing_ref=$3
  local test_id=$4
  local file_should_be_deleted=$(echo ${test_case} | jq -r '.file_deleted')
  if run_lakectl fs cat "lakefs://${repo}/${existing_ref}/file${test_id}" > /dev/null 2>&1 && ${file_should_be_deleted} ; then
    echo "Expected the file to be removed by the garbage collector but it has remained in the repository"
    return 1
  elif ! run_lakectl fs cat "lakefs://${repo}/${existing_ref}/file${test_id}" > /dev/null && ! ${file_should_be_deleted} ; then
    echo "Expected the file to remain in the repository but it was removed by the garbage collector"
    return 1
  fi
  for branch_props in $(echo ${test_case} | jq -r '.branches [] | @base64'); do
    branch_props=$(_jq ${branch_props})
    local days_ago=$(echo ${branch_props} | jq -r '.delete_commit_days_ago')
    if [[ ${days_ago} -gt -1 ]]; then
      local branch_name=$(echo ${branch_props} | jq -r '.branch_name')
      for location in \
        lakefs://${repo}/main/not_deleted_file1 \
        lakefs://${repo}/main/not_deleted_file2 \
        lakefs://${repo}/main/not_deleted_file3
      do
        if ! run_lakectl fs cat ${location} > /dev/null; then
          echo "expected ${location} to exist"
          return 1
        fi
      done
    fi
  done
}

#################################
######## Tests Execution ########
#################################
day_in_seconds=100000 # rounded up from 86400
current_epoch_in_seconds=$(date +%s)

failed_tests=()

prepare_for_gc() {
  local test_case=$1
  local test_id=$2
  local repo="${REPOSITORY}-${test_id}"

  run_lakectl repo create "lakefs://${repo}" "${STORAGE_NAMESPACE}/${repo}/"
  run_lakectl fs upload "lakefs://${repo}/main/not_deleted_file1" -s /local/gc-tests/sample_file
  run_lakectl fs upload "lakefs://${repo}/main/not_deleted_file2" -s /local/gc-tests/sample_file
  run_lakectl fs upload "lakefs://${repo}/main/not_deleted_file3" -s /local/gc-tests/sample_file
  run_lakectl commit "lakefs://${repo}/main" -m "add three files not to be deleted" --epoch-time-seconds 0

  run_lakectl branch create "lakefs://${repo}/a${test_id}" -s "lakefs://${repo}/main"
  run_lakectl fs upload "lakefs://${repo}/a${test_id}/file${test_id}" -s /local/gc-tests/sample_file
  file_existing_ref=$(run_lakectl commit "lakefs://${repo}/a${test_id}" -m "uploaded file ${test_id}" --epoch-time-seconds 0 | grep "ID: " | awk '{ print $2 }')
  run_lakectl branch create "lakefs://${repo}/b${test_id}" -s "lakefs://${repo}/${file_existing_ref}"
  echo "${file_existing_ref}" > "existing_ref_${test_id}.txt"
  echo "${test_case}" | jq --raw-output '.policy' > "policy_${test_id}.json"
  run_lakectl gc set-config lakefs://"${repo}" -f "/local/policy_${test_id}.json"
  delete_and_commit "${test_case}" "${repo}" "${test_id}"
}

do_case() {
  test_key=$1
  test_case=$(jq -r ".[${test_key}] | @base64" gc-tests/test_scenarios.json)
  test_case=$(_jq "${test_case}")
  test_id=$(echo "${test_case}" | jq -r '.id')
  repo="${REPOSITORY}-${test_id}"
  prepare_for_gc "${test_case}" "${test_id}"
  run_gc "${repo}"
}

test_keys=()
for test_case in $(jq -r 'to_entries | .[] | @base64' gc-tests/test_scenarios.json); do
    test_case=$(_jq "${test_case}")
    test_keys+=("$(echo "${test_case}" | jq -r '.key')")
done

export -f do_case
# Run GC jobs in parallel processes:
printf '%s\n' "${test_keys[@]}" | xargs -P 8 -n 1 -I {} bash -c 'do_case "$@"' _ {}

for test_case in $(jq -r '.[] | @base64' gc-tests/test_scenarios.json); do
    test_case=$(_jq ${test_case})
    test_id=$(echo "${test_case}" | jq -r '.id')
    test_description=$(echo "${test_case}" | jq -r '.description')
    file_existing_ref=$(cat "existing_ref_${test_id}.txt")
    if ! validate_gc_job "${test_case}" "${REPOSITORY}-${test_id}" "${file_existing_ref}" "${test_id}"; then
      failed_tests+=("${test_description}")
    else
      echo "Test: ${test_description} - SUCCEEDED"
    fi
    rm -f "policy_${test_id}.json"
    rm -f "existing_ref_${test_id}.txt"
done

if (( ${#failed_tests[@]} > 0)); then
  for ft in "${failed_tests[@]}"; do
      echo "Test: ${ft} - FAILED"
  done
  exit 1
fi

echo "Tests completed successfully"
