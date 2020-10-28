# Release Checklist

- [ ] [Master branch checks](https://github.com/treeverse/lakeFS/actions?query=branch%3Amaster) are green 
- [ ] Create a [new draft release](https://github.com/treeverse/lakeFS/releases/new)
    - Bump the version based on [SemVer](https://semver.org/) guidelines.
    - Make sure the version is `v` succeeded by the SemVer number. Example: `v1.1.14`.
    - If the change includes a migration, it is considered a new feature, and the minor version needs to be bumped.
- [ ] Verify that the [GoReleaser actions](https://github.com/treeverse/lakeFS/actions?query=workflow%3Agoreleaser) triggered by the new draft release completes successfully.
- [ ] Update the release notes (after goreleaser auto-generated list)
    - Remove test and documentation related stuff
    - Try to sort the list by importance
    - Update the description of items if needed so the reader will understand better what this release changes
    - Migration should be noted and documented outside the changelog (check the changelog for migrate: prefix):
    - How to run the migration if needed
    - Whether lakeFS service needs to be down while migration is running (backward compatibility)
- [ ] Uncheck 'This is a pre-release'
- [ ] Update [charts](https://github.com/treeverse/charts) repository (PR, Approve and Commit)
    - Bump the `version` patch level (`charts/lakefs/Chart.yaml`)
    - Update `appVersion` to the lakeFS version (`charts/lakefs/Chart.yaml`)
