# Release Checklist

- [ ] [Master branch checks](https://github.com/treeverse/lakeFS/actions?query=branch%3Amaster) are green 
- [ ] Create a [new draft release](https://github.com/treeverse/lakeFS/releases/new)
    - Bump the version based on SemVer guidelines [SemVer](https://semver.org/)
    - Make sure that version starts with 'v' and the semver number
    - Database migration is part of new feature and not a patch
- [ ] Verify that [GoReleaser actions](https://github.com/treeverse/lakeFS/actions?query=workflow%3Agoreleaser) triggered by new draft release complates successfully
- [ ] Update the release notes (after goreleaser auto-generated list)
    - Remove test and documentation related stuff
    - Try to sort the list by importance
    - Update the description of items if needed so the reader will understand better what this release changes
    - Migrateion should be noted and documented outside the changelog (check the changelog for migrate: prefix)
    - How to run the migrate if needed
    - Is lakeFS service needs to be down while migrate is running (backward compatibility)
- [ ] Uncheck the 'This is a pre-release'
- [ ] Update [charts](https://github.com/treeverse/charts) repository (PR, Approve and Commit)
    - Bump the `version` patch level (`charts/lakefs/Chart.yaml`)
    - Update `appVersion` to the lakeFS version (`charts/lakefs/Chart.yaml`)

