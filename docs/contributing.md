---
layout: default
title: Contributing
description: lakeFS community welcomes your contribution. To make the process as seamless as possible, we recommend reading this contribution guide first.
nav_order: 105
has_children: true
---

# Contributing to lakeFS

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure that we have all the necessary information to effectively respond to your bug report or contribution.

*Don't know where to start?* Reach out on the #dev channel on [our Slack](https://lakefs.io/slack) and we will help you get started. We also recommend this [free series](https://app.egghead.io/playlists/how-to-contribute-to-an-open-source-project-on-github){:target="_blank"} about contributing to OSS projects.
{: .note }

## Getting Started

Before you get started, we kindly ask that you:

* Check out the [code of conduct](https://github.com/treeverse/lakeFS/blob/master/CODE_OF_CONDUCT.md).
* Sign the [lakeFS CLA](https://cla-assistant.io/treeverse/lakeFS) when making your first pull request (individual / corporate)
* Submit any security issues directly to [security@treeverse.io](mailto:security@treeverse.io).
* Contributions should have an associated [GitHub issue](https://github.com/treeverse/lakeFS/issues/). 
* Before making major contributions, please reach out to us on the #dev channel on [Slack](https://lakefs.io/slack).
  We will make sure no one else is working on the same feature. 

## Setting up an Environment

*This section was tested on macOS and Linux (Fedora 32, Ubuntu 20.04) - Your mileage may vary*


Our [Go release workflow](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/goreleaser.yaml) holds the Go and Node.js versions we currently use under _go-version_ and _node-version_ compatibly. The Java workflows use [Maven 3.8.x](https://github.com/actions/runner-images/blob/bc22983319daa620b2ad01a74b68f6f462d86241/images/linux/Ubuntu2004-Readme.md) (but any recent version of Maven should work).

1. Install the required dependencies for your OS:
   1. [Git](https://git-scm.com/downloads)
   1. [GNU make](https://www.gnu.org/software/make/) (probably best to install from your OS package manager such as [apt](https://en.wikipedia.org/wiki/APT_(software)) or [brew](https://brew.sh/))
   1. [Docker](https://docs.docker.com/get-docker/)
   1. [Go](https://golang.org/doc/install)
   1. [Node.js & npm](https://www.npmjs.com/get-npm)
   1. [Maven](https://maven.apache.org/) to build and test Spark client codes.
   1. Java 8
      * Apple M1 users can install this from [Azul Zulu Builds for Java JDK](https://www.azul.com/downloads/?package=jdk). Builds for Intel-based Macs are available from [java.com](https://www.java.com/en/download/help/mac_install.html).
   1. *Optional* - [PostgreSQL 11](https://www.postgresql.org/docs/11/tutorial-install.html) (useful for running and debugging locally)
   1. *Optional* - [Rust & Cargo](https://www.rust-lang.org/tools/install) (only needed if you want to build the Delta Lake diff plugin)

1. [Clone](https://github.com/git-guides/git-clone) the [repository from GitHub](https://github.com/treeverse/lakeFS). 

    _This gives you read-only access to the repository. To contribute, see the next section._

1. Build the project:

   ```shell
   make build
   ```

   _Note: `make build` won't work for Windows users._

1. Make sure tests are passing. The following should not return any errors: 

   ```shell
   make test
   ```

## Before creating a pull request

1. Review this document in full.
1. Make sure there's an open [issue on GitHub](https://github.com/treeverse/lakeFS/issues) that this pull request addresses, and that it isn't labeled `x/wontfix`.
1. [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) the [lakeFS repository](https://github.com/treeverse/lakeFS).
1. If you're adding new functionality, create a new branch named `feature/<DESCRIPTIVE NAME>`.
1. If you're fixing a bug, create a new branch named `fix/<DESCRIPTIVE NAME>-<ISSUE NUMBER>`.

## Testing your change

Once you've made the necessary changes to the code, make sure the tests pass:

Run unit tests:

```shell
make test
```

Check that linting rules are passing. 

```shell
make checks-validator
```

You will need GNU diff to run this. On the macOS it can be installed with `brew install diffutils`
{: .note }

lakeFS uses [go fmt](https://golang.org/cmd/gofmt/) as a style guide for Go code.

Run system-tests:

```shell
make system-tests
```

Want to dive deeper into our system tests infrastructure? Need to debug the tests? Follow [this documentation](https://github.com/treeverse/lakeFS/blob/master/esti/docs/README.md).
{: .note }

## Submitting a pull request

Open a GitHub pull request with your change. The PR description should include a brief explanation of your change.
You should also mention the related GitHub issue. If the issue should be automatically closed after the merge, please [link it to the PR](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword).

After submitting your pull request, [GitHub Actions](https://github.com/treeverse/lakeFS/actions) will automatically run tests on your changes and make sure that your updated code builds and runs on Go 1.19.x.

Check back shortly after submitting your pull request to make sure that your code passes these checks. If any of the checks come back with a red X, then do your best to address the errors.

A developer from our team will review your pull request, and may request some changes to it. After the request is approved, it will be merged to our main branch.



## Documentation

Any contribution to the docs, whether it is in conjunction with a code contribution or as a standalone, is appreciated.

Documentation of features and changes in behaviour should be included in the pull request.
You can create separate pull requests for documentation changes only.

To learn how to contribute to the lakeFS documentation see the [README in the `/docs` folder of the lakeFS repository](https://github.com/treeverse/lakeFS/blob/master/docs/README.md). The README also includes details on how to build the documentation locally. 

## CHANGELOG.md

Any user-facing change should be labeled with `include-changelog`.
The PR title should contain a concise summary of the feature or fix and the description should have the GitHub issue number.
When we publish a new version of lakeFS, we will add this to the relevant version section of the changelog.
If the change should not be included in the changelog, label it with `exclude-changelog`.
