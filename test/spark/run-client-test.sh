#!/bin/bash -aux

run_lakectl() {
  docker-compose exec -T lakefs lakectl "$@"
}

run_gc () {
  docker-compose run  -v  ${CLIENT_JAR}:/client/client.jar -T --no-deps --rm spark-submit bash -c 'spark-submit --master spark://spark:7077 --class io.treeverse.clients.GarbageCollector   -c spark.hadoop.lakefs.api.url=http://docker.lakefs.io:8000/api/v1 -c spark.hadoop.lakefs.api.access_key=${TESTER_ACCESS_KEY_ID}    -c spark.hadoop.lakefs.api.secret_key=${TESTER_SECRET_ACCESS_KEY}     -c spark.hadoop.fs.s3a.access.key=${LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID}   -c spark.hadoop.fs.s3a.secret.key=${LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY} /client/client.jar $1 us-east-1'
}

clean_repo() {
  local repo=$1
  branch_names=$(run_lakectl branch list lakefs://${repo} | awk '{print $1}')
  for branch in ${branch_names}
  do
    if [[ "${branch}" != "main" ]]; then
      run_lakectl branch delete "lakefs://${repo}/${branch}" -y
    fi
  done
}

reset() {
  local repo=$1
  clean_repo ${repo}
  run_lakectl branch create "lakefs://${repo}/a" -s "lakefs://${repo}/main"
  run_lakectl fs upload lakefs://${repo}/a/file -s gc-tests/sample_file
  run_lakectl branch create "lakefs://${repo}/b" -s "lakefs://${repo}/a"
}

commit() {
  docker-compose exec -T lakefs lakectl commit lakefs://$1/$2 --allow-empty-message --epoch-time-seconds $3
}

delete_and_commit() {
  local test_case=$1
  local repo=$2
  echo ${test_case} | jq -c '.branches []' | while read branch_props; do
    local branch_name=$(echo ${branch_props} | jq -r '.branch_name')
    local days_ago=$(echo ${branch_props} | jq -r '.days_ago')
    if [[ ${days_ago} -gt -1 ]]
    then
      run_lakectl fs rm "lakefs://${repo}/${branch_name}/file"
      epoch_commit_date_in_seconds=$(( ${current_epoch_in_seconds} - ${day_in_seconds} * ${days_ago} ))
      run_lakectl commit lakefs://${repo}/${branch_name} --allow-empty-message --epoch-time-seconds ${epoch_commit_date_in_seconds}
    else   # This means that the branch should be deleted
      run_lakectl branch delete "lakefs://${repo}/${branch_name}" -y
    fi
  done
}

validate_gc_job() {
  local test_case=$1
  local repo=$2
  local file_should_be_deleted=$(echo ${test_case} | jq '.file_deleted')
  echo ${test_case} | jq -c '.branches []' | while read branch_props; do
    local branch_name=$(echo ${branch_props} | jq '.branch_name')
    local days_ago=$(echo ${branch_props} | jq '.days_ago')
    if [[ ${days_ago} -gt -1 ]]; then
      if run_lakectl fs cat lakefs://${repo}/${branch_name}/file > /dev/null 2>&1 ; then
        echo "Expected file to be removed by garbage collection"
        exit 1
      fi
      for location in \
        lakefs://${repo}/${branch_name}/not_deleted_file1 \
        lakefs://${repo}/${branch_name}/not_deleted_file2 \
        lakefs://${repo}/${branch_name}/not_deleted_file3
      do
        if ! run_lakectl fs cat ${location} > /dev/null; then
          echo "expected ${location} to exist"
          exit 1
        fi
      done
    fi
  done
}

################################
######## Test Execution ########
################################

day_in_seconds=86400
current_epoch_in_seconds=$(date +%s)
policy_tmp=$(mktemp)

trap 'rm -f -- $policy_tmp' INT TERM EXIT

run_lakectl fs upload lakefs://${REPOSITORY}/main/not_deleted_file1 -s gc-tests/sample_file
run_lakectl fs upload lakefs://${REPOSITORY}/main/not_deleted_file2 -s gc-tests/sample_file
run_lakectl fs upload lakefs://${REPOSITORY}/main/not_deleted_file3 -s gc-tests/sample_file

jq -c '.[]' gc-tests/test-scenarios/test_scenarios.json | while read test_case; do
  reset ${REPOSITORY}
  echo ${test_case} | jq --raw-output '.policy' > ${policy_tmp}/policy.json
  run_lakectl gc set-config lakefs://${REPOSITORY} -f ${policy_tmp}/policy.json
  delete_and_commit ${test_case} ${REPOSITORY}
  run_gc ${REPOSITORY}
  validate_gc_job ${test_case} ${REPOSITORY}
  rm ${policy_tmp}/policy.json
done
clean_repo ${REPOSITORY}

run_lakectl fs rm "lakefs://${REPOSITORY}/main/not_deleted_file1"
run_lakectl fs rm "lakefs://${REPOSITORY}/main/not_deleted_file2"
run_lakectl fs rm "lakefs://${REPOSITORY}/main/not_deleted_file3"
