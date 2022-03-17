# Protected Branches

## Requirements

1. As a lakeFS user, I should be able to mark branches as "protected" - from the UI, CLI and API.
1. Writes and commits on protected branches will be blocked.
1. Forbidden actions to protected branches should fail with a meaningful error message.
1. The implementation should be flexible to adding more constraint types in the future (example: require PR before merging).

## Where to save the model

### Suggestion 1 (@johnnyaug)

Save protection rules as a JSON object under the repository's __lakefs_. This is similar to what we do with retention rules.

Pros:
- rules can be patterns and not just concrete branches, similar to GitHub's protected branches.
- native to the storage
- flexible: structure can easily change to accommodate for more complex constraints. 

Cons:
- UI will probably be a multiline textbox where you can edit the json, otherwise consistency problems.
- CLI will also need to get the full JSON, or we need to do some locking on the server-side to add/remove protection rules.

### Suggestion 2 (@ariels)

Branch protection rules are part of the repository data, saved under a dedicated lakeFS directory, similar to GitHub's (and our) Actions.

Pros:
- Reuse the Hooks mechanism for enforcement.
- Use lakeFS IAM to give permissions to edit protection rules.

Cons:
- As a repository owner, I need to manage rules in multiple branches. For example, if I want to add rules to existing branches, I need to merge these rules to all of my developer's branches.
- Can currently only be used to block commits, and not the staging area.
 
Pro/con:
- Changing protection rules is a commit visible in the log

### Decision

After a meeting discussing the matter, we decided to go with suggestion #1, but to add a repository-level settings feature to be used in order to store the branch protection rules. See the [issue](https://github.com/treeverse/lakeFS/issues/2406).

## Implementation - Suggestion 1

### Example Branch Protection JSON

```json
[
	{
		"branch_name_pattern": "main",
		"blocked_actions": ["staging_write", "commit"]
	},
	{
		"branch_name_pattern": "stable/*",
		"blocked_actions": ["staging_write", "commit"]
	}
]
```

When a branch matches a protection rule, the operations described in the rule's `blocked_actions` will be blocked. 
Blocked actions can be:
1. `staging_write`: any write operation on the staging area, including: upload, delete, revert changes.
2. `commit`: a simple commit, not including merges and reverts.

For now, we will always add both blocked actions for all rules. In the future we will allow more flexibility.

### Runtime

The enforcement of the protection depends on the constraint type.
In the future we may want to add an option to force these operations.

#### Constraint type: block commits

* When performing a commit, the protection rules of the repository will be fetched from the storage.
* Right before executing the pre-commit hooks, check whether the branch name matches any of the protection rules.
* If so, the commit will fail with a dedicated error type.

#### Constraint type: block staging area

* In Graveler, before every Staging Manager write operation (Set, Drop, DropKey) - fetch the rules.
* If the branch name matches a rule, fail the operation with a dedicated error type.
