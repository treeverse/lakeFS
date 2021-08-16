# Protected Branches

## Requirements

1. As a lakeFS user, I should be able to mark branches as "protected" - from the UI, CLI and API.
1. Descision: Protected branches will be subject to one of the following constraints:
   1. (either) they cannot be directly committed to. Only merges are allowed.
   1. (or) their staging area is blocked - cannot directly make any changes.
1. Forbidden actions to protected branches should fail with a meaningful error message.
1. The implmenetation should be flexible to adding more constraint types in the future (example: require PR before merging).
1. Force options - blocked operations should have a reasonable way to be forced. In the future, the extent to which they can be forced may also be configurable (similarly to GitHub).

## Where to save the model
Suggestion: save protection rules as a JSON object under the repository's _lakefs_. This is similar to what we do with retention rules.

Pros:
- rules can be patterns and not just concrete branches, similar to GitHub's protected branches.
- native to the storage
- flexible: structure can easily change to accommodate for more complex constraints. 

Cons:
- UI will probably be a multiline textbox where you can edit the json, otherwise consistency problems.
- CLI will also need to get the full JSON, or we need to do some locking on the server-side to add/remove protection rules.
