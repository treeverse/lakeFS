---
title: Pull Requests
description: Improve collaboration over data with Pull Requests.
parent: How-To
redirect_from: 
  - /reference/pull_requests.html
---

# Pull Requests

A pull request is a proposal to merge a set of changes from one branch into another.
In a pull request, collaborators can review the proposed set of changes before they integrate the changes.
Pull requests display the differences, or diffs, between the content in the source branch and the content in the target branch.

{% include toc.html %}

## Open a Pull Request

Create a branch, and make all the necessary changes in that branch.
When your changes are ready for review, head over to the _Pull Requests_ tab in your repository.
Choose your source branch and target branch, add a title and description (optional, and markdown is supported).

![Open Pull Request]({{ site.baseurl }}/assets/img/pull-request-open.png)

When ready, click _Create Pull Request_. You will be redirected to the newly created pull request page.

## Review Changes

Run validation checks or automated data quality tests to ensure that the changes meet your standards.

![Review Pull Request]({{ site.baseurl }}/assets/img/pull-request-review.png)

Every Pull Request is assigned a unique ID. You can share the Pull Request's URL with others to review the change.

As with any lakeFS reference, reviewers can take the source branch, query, test and modify it as necessary prior to merging.

## Merge or Close

Once the review is complete and all checks have passed, click the _Merge pull request_ button to merge the changes into the target branch.

![Merged Pull Request]({{ site.baseurl }}/assets/img/pull-request-merged.png)

The data is now updated in a controlled and transparent manner.

If the changes are not ready to be merged, you can close the pull request without merging the changes, by clicking the _Close pull request_ button.

## View Pull Requests

You can view all open and closed pull requests in the _Pull Requests_ tab in your repository.
The tabs (_Open_, _Closed_) allow you to filter the list of pull requests according to their status.