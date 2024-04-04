#!/bin/bash

REPOSITORY=${REPOSITORY:-example}

# Run Export without previous commit
echo "Current working directory: ${WORKING_DIRECTORY}"
echo "upload file_one"
docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_one.txt --source /local/file_one.txt
echo "res $?"

echo "export no previous commit"
docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --branch="main"
echo "res $?"

# Validate export
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

echo "ls"
docker compose exec -T lakefs lakectl fs ls --recursive --no-color lakefs://${REPOSITORY}/main/ | awk '{print $8}' | sort > ${lakectl_out}
echo "res $?"

n=$(grep -o "/" <<< ${EXPORT_LOCATION} | wc -l)

if  [[ "${EXPORT_LOCATION}" == */ ]]
then
  n=$((n-2))
else
  n=$((n-1))
fi

echo "aws ls"
aws s3 ls --recursive ${EXPORT_LOCATION} | awk '{print $4}'| cut -d/ -f ${n}-  | grep -v EXPORT_ | sort > ${s3_out}
echo "res $?"

if ! diff ${lakectl_out} ${s3_out}; then
  echo "export location and lakefs should contain same objects"
  exit 1
fi

# Run Export with previous commit - add file and also delete an existing file
echo "export previous commit"
docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_two.txt --source /local/file_two.txt
echo "res $?"
docker compose exec -T lakefs lakectl fs rm lakefs://${REPOSITORY}/main/a/file_one.txt
echo "res $?"
docker compose exec -T lakefs lakectl commit lakefs://${REPOSITORY}/main --message="removed file_one and added file_two"
echo "res $?"

docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --branch="main" --prev_commit_id="some_commit"
echo "res $?"

# Validate sync
lakectl_out=$(mktemp)
s3_out=$(mktemp)
trap 'rm -f -- $s3_out $lakectl_out' INT TERM EXIT

echo "ls"
docker compose exec -T lakefs lakectl fs ls --recursive --no-color lakefs://${REPOSITORY}/main/ | awk '{print $8}' | sort > ${lakectl_out}
echo "res $?"

echo "aws ls"
aws s3 ls --recursive ${EXPORT_LOCATION} | awk '{print $4}'| cut -d/ -f ${n}-  | grep -v EXPORT_ | sort > ${s3_out}

if ! diff ${lakectl_out} ${s3_out}; then
  echo "export location and lakefs should contain same objects"
  exit 1
fi

# Run Export with commit_id reference
echo "export with commit_id"
docker compose exec -T lakefs lakectl fs upload lakefs://${REPOSITORY}/main/a/file_three.txt --source /local/file_three.txt
echo "res $?"
commit_id=$(docker compose exec -T lakefs lakectl commit lakefs://${REPOSITORY}/main --message="added file_three" | sed -n 4p | awk '{print $2}')

docker compose --project-directory ${WORKING_DIRECTORY} run --rm lakefs-export ${REPOSITORY} ${EXPORT_LOCATION} --commit_id="$commit_id"
echo "res $?"

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