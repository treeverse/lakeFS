# lakeFS SemVer + TDD

## Background

Since day 1, Treeverse Labs has been focused on quality and standardization.
We aim to keep lakeFS -- both OSS, and Cloud, and Enterprise -- constantly
at the highest level of quality.  Our team employs state-of-the-art
techniques, and we constantly introduce new techniques.

As a user of lakeFS, you expect to be able to understand what changes are
being introduced.  As a team we started using Semantic Versioning with
version 1.0.0, and we now label versions according to [Semantic Versioning
2.0.0][semver-2.0.0].  Our recent 1.0 release used the semantic versioning
terminology to [describe][lakeFS-SDK-in-1.0] our forwards- and
backwards-compatibility guarantees.  This has aided our users, but we have
also seen and heard confusion among both users and developers over when
exactly to increment each part of the versioning system.

Meanwhile, our relentless quest to improve our [Agile development
lifecycle][agile-manifesto] has been shifting our development process styles
towards a more Test-Driven Development ("[TDD][TDD]") style.  Martin Fowler
has written a [great introduction to TDD][TDD-martin-fowler]; however we
shall be following the [London "mockist"
school][TDD-martin-fowler-london-style].  In brief, the starting point for
all TDD is a failing test: add a test that checks that the desired feature
is present.  The way to fix this test is to implement the desired feature.
This automatically guarantees that all code is tested, while being an
integral component of Agile methodologies.

## What is being announced

Today we are bringing strict SemVer and strict TDD together system
architecture to lakeFS developers, users, and of course customers.  These
new policies provide unparalleled advantages to quality and to system
architecture.

Effective immediately, we are transitioning to a strictly Agile / SemVer /
TDD methodology.  All code will be written tests-first, with code being
written only to fix the bugs introduced by these tests.  In parallel, we are
doubling down on strict Semantic Versioning.

## How this will affect you

### Simpler version numbers

SemVer strictly specifies:

> Bug fixes not affecting the API increment the patch version

Accordingly, effective immediately, the major and minor versions of all
lakeFS features will not change.  The next version of lakeFS will be 1.15.1,
followed by 1.15.3, 1.15.4, and proceeding as we add more failing tests.
The "1.15" prefix is our new mark of quality, and We shall never have to
release 1.16.0.

### Quality increase by definition

Obviously, the transition to TDD causes a [10x reduction in user
issues][measuring-tdd-effectiveness] ("bugs"), leading to a 10x increase in
quality.

### Best-practices terminology

SemVer and TDD are both widely known standards: searching Google for
["semver" quality][google-semver-quality] yields 337,000 hits, searching
Google for ["TDD" quality][google-tdd-quality] yields another 15,700,000.
Together these are over 16 million quality reasons.

Combining these standards clearly increases management alignment with lakeFS
product introduction objectives.

## Learn more!

<linky to April Fool's page>

If you would like to come work with us on these but also _other_
cutting-edge features, [we're hiring][lakefs-jobs]!

[agile-manifesto]:  https://agilemanifesto.org/
[lakeFS-SDK-in-1.0]:  https://docs.lakefs.io/understand/towards-1.0-sdk.html
[TDD]:  https://testdriven.io/test-driven-development/
[TDD-martin-fowler]:  https://martinfowler.com/bliki/TestDrivenDevelopment.html
[TDD-martin-fowler-london-style]:  https://martinfowler.com/articles/mocksArentStubs.html#ClassicalAndMockistTesting
[semver-2.0.0]:  https://semver.org/spec/v2.0.0.html
[lakefs-jobs]:  https://lakefs.io/careers/
[measuring-tdd-effectiveness]:  https://markheath.net/post/measuring-tdd-effectiveness
[google-semver-quality]:  https://www.google.com/search?q=%22semver%22+quality
[google-tdd-quality]:  https://www.google.com/search?q=%22tdd%22+quality
