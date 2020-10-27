# Release Checklist

- [ ] Make sure [master branch checks](https://github.com/treeverse/lakeFS/actions?query=branch%3Amaster) are green 
- [ ] Create a [new draft release](https://github.com/treeverse/lakeFS/releases/new)
- [ ] Bump the version based on SemVer guidelines [SemVer](https://semver.org/)
    Make sure that version starts with 'v' and the semver number
    Database migration is part of new feature and not a patch
- [ ] Verify that [GoReleaser actions](https://github.com/treeverse/lakeFS/actions?query=workflow%3Agoreleaser) triggered by new draft release complates successfully
- [ ] Update the new release notes
    Remove test and documentation related stuff
    Try to sort the list by importance
    Update the description of items if needed so the reader will understand better what this release changes
- [ ] Uncheck the 'This is a pre-release'
- [ ] Update [charts](https://github.com/treeverse/charts) repository (PR, Approve and Commit)
    Edit `charts/lakefs/Chart.yaml`
     - Bump the `version` patch level
     - Update the `appVersion` to the lakeFS version we released

