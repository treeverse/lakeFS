# Protected Branches

## Requirements

1. As a lakeFS user, I should be able to mark branches as "protected" - from the UI, CLI and API.
1. Protected branches cannot be directly committed to. Only merges are allowed into protected branches.
1. Commits to protected branches should fail with a meaningful error message.

## Where to save the model

The data regarding which branch is protected needs to be saved somewhere.
It needs to be fetched at runtime, probably before a commit is performed.

Here are two suggested options, feel free to add your own:

### Option 1: branches table in postgres

Add a boolean column to the branches table, denoting whether the branch is protected.

Pros:
- no consistency problems
- no overhead: branch is anyway fetched from the database before a commit.

Cons:
- yet another dependency on postgres
- no pattern rules - every branch needs to marked as protected separately.

Option 2: save it as a JSON array in the storage

Add it as a file under the repository's _lakefs directory.

Pros:
- rules can be patterns and not just concrete branches, similar to GitHub's protected branches.
- native to the storage

cons:
- UI will probably be a multiline textbox where you can edit the json, otherwise consistency problems.
- CLI will also need to get the full JSON, or we need to do some locking on the server-side to add/remove protection rules.
