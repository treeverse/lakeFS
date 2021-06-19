---
layout: default
title: Contributing
description: >-
  lakeFS community welcomes your contribution. To make the process as seamless
  as possible, we recommend you read this contribution guide.
nav_order: 55
has_children: false
---

# Contributing

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary information to effectively respond to your bug report or contribution..

If you don't know where to start, please [join our community on Slack](https://lakefs.io/slack) and ask us. We will help you get started!

## Ground Rules

Before you get started, we ask that you:

* Check out the [code of conduct](https://github.com/treeverse/lakeFS/blob/master/CODE_OF_CONDUCT.md). 
* Sign the [lakeFS CLA](https://cla-assistant.io/treeverse/lakeFS) when making your first pull request \(individual / corporate\)
* Submit any security issues directly to [security@treeverse.io](mailto:security@treeverse.io)

## Getting Started

Want to report a bug or request a feature? Please [open an issue](https://github.com/treeverse/lakeFS/issues/new)

Working on your first Pull Request? You can learn how from this free series, [How to Contribute to an Open Source Project on GitHub](https://app.egghead.io/playlists/how-to-contribute-to-an-open-source-project-on-github).

## Setting up an Environment

_This section was tested on macOS and Linux \(Fedora 32, Ubuntu 20.04\) - Your mileage may vary_

Our [Go release workflow](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/goreleaser.yaml) holds the Go and Node.js versions we currently use under _go-version_ and _node-version_ compatibly.

1. Install the required dependencies for your OS:
   1. [Git](https://git-scm.com/downloads)
   2. [GNU make](https://www.gnu.org/software/make/) \(probably best to install from your OS package manager such as apt or brew\)
   3. [Docker](https://docs.docker.com/get-docker/)
   4. [Go](https://golang.org/doc/install)
   5. [Node.js & npm](https://www.npmjs.com/get-npm)
   6. _Optional_ - [PostgreSQL 11](https://www.postgresql.org/docs/11/tutorial-install.html) \(useful for running and debugging locally\)
2. Install statik:

   ```text
   go get github.com/rakyll/statik
   ```

   Make sure `(go env GOPATH)/bin` is in your `$PATH` \(or at least, that the `statik` binary is\).

3. Clone the repository from [https://github.com/treeverse/lakeFS](https://github.com/treeverse/lakeFS) \(gives you read-only access to the repository. To contribute, see the next section\).
4. Build the project:

   ```text
   make build
   ```

5. Make sure tests are passing:

   ```text
   make test
   ```

## Before creating a pull request

1. Review this document in full
2. Make sure there's an open issue on GitHub that this pull request addresses, and that it isn't labeled `x/wontfix`
3. Fork the [lakeFS repository](https://github.com/treeverse/lakeFS)
4. If you're adding new functionality, create a new branch named `feature/<DESCRIPTIVE NAME>`
5. If you're fixing a bug, create a new branch named `fix/<DESCRIPTIVE NAME>-<ISSUE NUMBER>`

## Creating a pull request

Once you've made the necessary changes to the code, make sure tests pass:

```text
   make test
```

Check linting rules are passing:

```text
   make checks-validator
```

lakeFS uses [go fmt](https://golang.org/cmd/gofmt/) as a style guide for Go code.

## After submitting your pull request

After submitting your pull request, [GitHub Actions](https://github.com/treeverse/lakeFS/actions) will automatically run tests on your changes and make sure that your updated code builds and runs on Go 1.16.2.

Check back shortly after submitting your pull request to make sure that your code passes these checks. If any of the checks come back with a red X, then do your best to address the errors.

