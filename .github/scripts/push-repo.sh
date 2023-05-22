#!/bin/bash
set -e
set -u

if [ -n "${SSH_DEPLOY_KEY:=}" ]
then
    echo "[+] Using SSH_DEPLOY_KEY"
    mkdir --parents "$HOME/.ssh"
    DEPLOY_KEY_FILE="$HOME/.ssh/deploy_key"
    echo "${SSH_DEPLOY_KEY}" > "$DEPLOY_KEY_FILE"
    chmod 600 "$DEPLOY_KEY_FILE"
    SSH_KNOWN_HOSTS_FILE="$HOME/.ssh/known_hosts"
    ssh-keyscan -H "github.com" > "$SSH_KNOWN_HOSTS_FILE"
    export GIT_SSH_COMMAND="ssh -i "$DEPLOY_KEY_FILE" -o UserKnownHostsFile=$SSH_KNOWN_HOSTS_FILE"
else
    echo "::error::SSH_DEPLOY_KEY is empty. Please fill one!"
    exit 1
fi

git lfs install
git config --global user.email "$USER_EMAIL"
git config --global user.name "$USER_NAME"
# workaround for https://github.com/cpina/github-action-push-to-another-repository/issues/103
git config --global http.version HTTP/1.1

CLONE_DIR=$(mktemp -d)
git clone --single-branch --depth 1 --branch "$TARGET_BRANCH" "$GIT_CMD_REPOSITORY" "$CLONE_DIR"

# $TARGET_DIRECTORY is '' by default
ABSOLUTE_TARGET_DIRECTORY="$CLONE_DIR/$TARGET_DIRECTORY/"
echo "[+] Creating (now empty) $ABSOLUTE_TARGET_DIRECTORY"
mkdir -p "$ABSOLUTE_TARGET_DIRECTORY"

echo "[+] Checking if local $SOURCE_DIRECTORY exist"
if [ ! -d "$SOURCE_DIRECTORY" ]; then
    echo "ERROR: $SOURCE_DIRECTORY does not exist"
    exit 1
fi

echo "[+] Copying contents of source repository folder $SOURCE_DIRECTORY to folder $TARGET_DIRECTORY in git repo"
cp -ra "$SOURCE_DIRECTORY"/. "$CLONE_DIR/$TARGET_DIRECTORY"
cd "$CLONE_DIR"

ORIGIN_COMMIT="https://github.com/$GITHUB_REPOSITORY/commit/$GITHUB_SHA"
COMMIT_MESSAGE="Update from ${ORIGIN_COMMIT}"

echo "[+] Set directory is safe ($CLONE_DIR)"
git config --global --add safe.directory "$CLONE_DIR"

echo "[+] Adding git commit"
git add .

echo "[+] git diff-index:"
# git diff-index : to avoid doing the git commit failing if there are no changes to be commit
git diff-index --quiet HEAD || git commit --message "$COMMIT_MESSAGE"

echo "[+] Pushing git commit"
# --set-upstream: sets de branch when pushing to a branch that does not exist
git push "$GIT_CMD_REPOSITORY" --set-upstream "$TARGET_BRANCH"

