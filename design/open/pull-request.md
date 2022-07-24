# Pull-request Proposal

Enable a way to submit changes to a branch by creating a request to merge changes.
The pull-request captures the merge operation we request to apply.

### Goals

- Capture request to merge a branch or specific reference
- Mechanism to review, discuss and approve changes before applying to a branch
- Automation using actions can verify / provide feedback on pull-request

### How it will work

Introduce a new resource called pull-request at the repository level.
The pull-request will hold owner (the user who created the PR), assignee, reviewers the source branch/reference and the target branch.
Each reviewer can set approved/reject (or clear his response) on the PR.
PR can be merge operation is enabled when at least one reviewer approves or when no reviewers are assigned.
PR can be closed at any time.
PR will not be updated when new commits into the source branch.
Merge PR will perform a merge operation and merge the PR as merged. If a conflict or an error occurs while merging, it will be returned as a response to the request.
When a repository is deleted, all pull-requests will be deleted with it.

### Other features to consider

- PR auto-update with latest branch commits. PR will include the base commit and all the changes until the latest commit on branch.
- Multiple reviews with text from each reviewer. Can be even per level of commit the current PR addresses.
- New hook can be trigger when PR is updated. Action triggered by PR will be considered as reviewers and the status is approved/rejects based on a successful run.

