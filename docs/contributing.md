---
layout: default
title: Contributing
nav_order: 10
has_children: false
---

# Contributing to lakeFS

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary information to effectively respond to your bug report or contribution..

If you don't know where to start, please [join our community on Slack](https://join.slack.com/t/lakefs/shared_invite/zt-g86mkroy-186GzaxR4xOar1i1Us0bzw) and ask us. We will help you get started! 

## Ground Rules

Before you get started, we ask that you:

* Check out the [code of conduct](https://github.com/treeverse/lakeFS/blob/master/CODE_OF_CONDUCT.md). 
* Sign the [lakeFS CLA](https://cla-assistant.io/treeverse/lakeFS) when making your first pull request (individual / corporate)
* Submit any security issues directly to [security@treeverse.io](mailto:security@treeverse.io)

## Get ready for Hacktoberfest!

Contribute to open-source projects throughout October 2020 by stepping up to Digital Ocean's annual tradition of hosting [Hacktoberfest](https://hacktoberfest.digitalocean.com/).  We _always_ welcome pull requests to lakeFS - but throughout October your pull requests to open source projects can get you some cool swag (stickers & t-shirt).  Check out our [contributing guide](https://docs.lakefs.io/contributing) and join our [slack channel](https://join.slack.com/t/lakefs/shared_invite/zt-g86mkroy-186GzaxR4xOar1i1Us0bzw) for help, more ideas, and discussions. 

Not sure what to do?  We marked some issues that could get you started quickly on our [Hacktoberfest list](https://github.com/treeverse/lakeFS/issues?q=is%3Aissue+is%3Aopen+label%3Ahacktoberfest).

## Getting Started

Want to report a bug or request a feature? Please [open an issue](https://github.com/treeverse/lakeFS/issues/new)

Working on your first Pull Request? You can learn how from this free series, [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github).

## Setting up an Environment

*This section was tested on macOS and Linux (Fedora 32, Ubuntu 20.04) - Your mileage may vary*

Our [Go release workflow](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/goreleaser.yaml) holds under _go-version_ the Go version we currently use.

1. Install the required dependencies for your OS:
    1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
    1. [GNU make](https://www.gnu.org/software/make/) (probably best to install from your OS package manager such as apt or brew)
    1. [Docker](https://docs.docker.com/get-docker/)
    1. [Go](https://golang.org/doc/install)
    1. [Node.js & npm](https://www.npmjs.com/get-npm)
    1. *Optional* - [PostgreSQL 11](https://www.postgresql.org/docs/11/tutorial-install.html) (useful for running and debugging locally)
1. Install statik:
   
   ```shell
   go get github.com/rakyll/statik 
   ```
   
   Make sure `(go env GOPATH)/bin` is in your `$PATH` (or at least, that the `statik` binary is). 
   
1. Clone the repository from https://github.com/treeverse/lakeFS
1. Build the project:
   
   ```shell
   make build
   ```
   
1. Make sure tests are passing:
   
   ```shell
   make test 
   ```

## Before creating a pull request

1. Review this document in full
1. Make sure there's an open issue on GitHub that this pull request addresses, and that it isn't labelled `WON'T-FIX`
1. Fork the [lakeFS repository](https://github.com/treeverse/lakeFS)
1. If you're adding new functionality, create a new branch named `feature/<DESCRIPTIVE NAME>`
1. If you're fixing a bug, create a new branch named `fix/<DESCRIPTIVE NAME>-<ISSUE NUMBER>`

## Creating a pull request

Once you've made the necessary changes to the code, make sure tests pass:

   ```shell
   make test 
   ```

Check listing rules are passing:

   ```shell
   make checks-validator
   ```

lakeFS uses [go fmt](https://golang.org/cmd/gofmt/) as a style guide for Go code.


## After submitting your pull request

After submitting your pull request, [GitHub Actions](https://github.com/treeverse/lakeFS/actions) will automatically run tests on your changes and make sure that your updated code builds and runs on Go 1.14.

Check back shortly after submitting your pull request to make sure that your code passes these checks. If any of the checks come back with a red X, then do your best to address the errors.
