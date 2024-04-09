#!/bin/bash

REPOSITORY=${REPOSITORY:-example}


run_cmd_and_validate() {
    echo $1
    echo "Running: $2"
    newVariable=$(eval $2)
    if [[ $? != "0" ]] 
    then
      echo "FAILED!" && exit 1
    fi
    echo "Output:"
    echo $newVariable
}

# Run Export without previous commit
echo "Current working directory: ${WORKING_DIRECTORY}"
run_cmd_and_validate "upload file_one" "docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_one.txt --source /local/file_one.txt"

run_cmd_and_validate "export no previous commit" "docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --branch=main"

# Validate export
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

docker compose exec -T lakefs lakectl fs ls --recursive --no-color lakefs://${REPOSITORY}/main/ | awk '{print $8}' | sort > ${lakectl_out}

n=$(grep -o "/" <<< ${EXPORT_LOCATION} | wc -l)

if  [[ "${EXPORT_LOCATION}" == */ ]]
then
  n=$((n-2))
else
  n=$((n-1))
fi

aws s3 ls --recursive ${EXPORT_LOCATION} | awk '{print $4}'| cut -d/ -f ${n}-  | grep -v EXPORT_ | sort > ${s3_out}

echo $(cat $lakectl_out)
echo $(cat $s3_out)

if ! diff ${lakectl_out} ${s3_out}; then
  echo "export location and lakefs should contain same objects"
  exit 1
fi

# Run Export with previous commit - add file and also delete an existing file
run_cmd_and_validate "upload file_two" "docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_two.txt --source /local/file_two.txt"
run_cmd_and_validate "delete file_one" "docker compose exec -T lakefs lakectl fs rm lakefs://${REPOSITORY}/main/a/file_one.txt"
run_cmd_and_validate "commit changes" "docker compose exec -T lakefs lakectl commit lakefs://${REPOSITORY}/main --message='removed file_one and added file_two'"
run_cmd_and_validate "export previous commit" "docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --branch=main --prev_commit_id='some_commit'"

# Validate sync
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

docker compose exec -T lakefs lakectl fs ls --recursive --no-color lakefs://${REPOSITORY}/main/ | awk '{print $8}' | sort > ${lakectl_out}
aws s3 ls --recursive ${EXPORT_LOCATION} | awk '{print $4}'| cut -d/ -f ${n}-  | grep -v EXPORT_ | sort > ${s3_out}

if ! diff ${lakectl_out} ${s3_out}; then
  echo "export location and lakefs should contain same objects"
  exit 1
fi

# Run Export with commit_id reference
run_cmd_and_validate "upload file_three" "docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_three.txt --source /local/file_three.txt"

commit_id=$(docker compose exec -T lakefs lakectl commit lakefs://${REPOSITORY}/main --message="added file_three" | sed -n 4p | awk '{print $2}')

# We should not validate the exit code - since it is erroneous 
echo "run export"
docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --commit_id=$commit_id

echo "commit_id $commit_id"

# Validate sync
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

echo "ls"
docker compose exec -T lakefs lakectl fs ls --recursive --no-color lakefs://${REPOSITORY}/main/ | awk '{print $8}' | sort > ${lakectl_out}
echo "res $?"

echo "aws ls"
aws s3 ls --recursive ${EXPORT_LOCATION} | awk '{print $4}'| cut -d/ -f ${n}-  | grep -v EXPORT_ | sort > ${s3_out}
echo "res $?"

if ! diff ${lakectl_out} ${s3_out}; then
  echo "export location and lakefs should contain same objects"
  exit 1
fi

# Delete files at destination in case of multiple runs, each one produce output under different folder
aws s3 rm ${EXPORT_LOCATION} --recursive
echo "res $?"