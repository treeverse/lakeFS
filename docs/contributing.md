---
layout: default
title: Contributing
description: lakeFS community welcomes your contribution. To make the process as seamless as possible, we recommend reading this contribution guide first.
nav_order: 55
has_children: false
---

# Contributing to lakeFS

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure that we have all the necessary information to effectively respond to your bug report or contribution.

*Don't know where to start?* Reach out on the #dev channel on [our Slack](https://lakefs.io/slack) and we will help you get started. We also recommend this [free series](https://app.egghead.io/playlists/how-to-contribute-to-an-open-source-project-on-github){:target="_blank"} about contributing to OSS projects.
{: .note .note-info }

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


Our [Go release workflow](https://github.com/treeverse/lakeFS/blob/master/.github/workflows/goreleaser.yaml) holds the Go and Node.js versions we currently use under _go-version_ and _node-version_ compatibly. The Java workflows use [Maven 3.8.1](https://github.com/actions/virtual-environments/blob/main/images/linux/Ubuntu2004-README.md) (but any recent version of Maven should work).

1. Install the required dependencies for your OS:
   1. [Git](https://git-scm.com/downloads)
   1. [GNU make](https://www.gnu.org/software/make/) (probably best to install from your OS package manager such as apt or brew)
   1. [Docker](https://docs.docker.com/get-docker/)
   1. [Go](https://golang.org/doc/install)
   1. [Node.js & npm](https://www.npmjs.com/get-npm)
   1. [Maven](https://maven.apache.org/) to build and test Spark client codes.
   1. *Optional* - [PostgreSQL 11](https://www.postgresql.org/docs/11/tutorial-install.html) (useful for running and debugging locally)

   * With Apple M1, you can install Java from [Azul Zulu Builds for Java JDK](https://www.azul.com/downloads/?package=jdk).

1. Clone the repository from https://github.com/treeverse/lakeFS (gives you read-only access to the repository. To contribute, see the next section).
1. Build the project:

   ```shell
   make build
   ```

Note: Make build won't work for Windows user for building lakeFS.

1. Make sure tests are passing:

   ```shell
   make test
   ```

## Before creating a pull request

1. Review this document in full.
1. Make sure there's an open issue on GitHub that this pull request addresses, and that it isn't labeled `x/wontfix`.
1. Fork the [lakeFS repository](https://github.com/treeverse/lakeFS).
1. If you're adding new functionality, create a new branch named `feature/<DESCRIPTIVE NAME>`.
1. If you're fixing a bug, create a new branch named `fix/<DESCRIPTIVE NAME>-<ISSUE NUMBER>`.

## Testing your change

Once you've made the necessary changes to the code, make sure the tests pass:

Run unit tests:

```shell
make test
```

Check that linting rules are passing:

```shell
make checks-validator
```
lakeFS uses [go fmt](https://golang.org/cmd/gofmt/) as a style guide for Go code.

Run system-tests:

```shell
make system-tests
```

Want to dive deeper into our system tests infrastructure? Need to debug the tests? Follow [this documentation](https://github.com/treeverse/lakeFS/blob/master/esti/docs/README.md).
{: .note .note-info }

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

📝  Notice! lakeFS documentation is written using Markdown. make sure to familiarize yourself with the [Markdown Guide](https://www.markdownguide.org/basic-syntax/#heading-best-practices).

Customizing the lakeFS docs site should follow the following guidelines: [Just The Docs Customization](https://just-the-docs.github.io/just-the-docs/docs/customization/) and style-guide.

### lakeFS Style Guide:
* Don't use unnecessary tech jargon or vague/wordy constructions - keep it friendly, not condescending.
* Be inclusive and welcoming - use gender-neutral words and pronouns when talking about abstract people like developers).
* Replace complex expressions with simpler ones.
* Keep it short - 25-30 words max per sentence.  Otherwise, your readers might get lost on the way. 
* Use active voice instead of passive. For example: This feature can be used to do task X. vs. You can use this feature to do task X. The second one reads much better, right?
* You can explain things better by including examples. Show, not tell. Use illustrations, images, gifs, code snippets, etc.
* Establish a visual hierarchy to help people quickly find the information they need. Use text formatting to create levels of title and subtitle (such as h1 to h6 headings in HTML).

### Test your changes localy
To render the documentation locally and preview changes, use the following command and [browse the documentation locally](http://localhost:4000):

```sh
cd docs
docker run --rm -p 4000:4000 --volume="$PWD:/srv/jekyll:Z" -it jekyll/jekyll:3.8 jekyll serve
```




### CHANGELOG.md

Any user-facing change should be labeled with `include-changelog`.
The PR title should contain a concise summary of the feature or fix and the description should have the GitHub issue number.
When we publish a new version of lakeFS, we will add this to the relevant version section of the changelog.
If the change should not be included in the changelog, label it with `exclude-changelog`.
