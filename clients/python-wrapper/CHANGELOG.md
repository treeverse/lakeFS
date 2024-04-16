# Changelog

## Unreleased

## v0.6.0

:new: What's new:

- Support multiple pydantic versions (#7682)

## v0.5.0

:new: What's new:

- Short-lived Token Authentication: Initiate lakeFS Python client sessions securely using external IdPs with the new federated "Assume Role with Web Identity" feature. (#7620)

:bug: Bugs fixed:

- Fix import manager run race #7607

## v0.4.1

:new: What's new:

- Allow passing args/kwargs to lakefs.repository() (#7470)

:bug: Bugs fixed:

- Fix Client SSL configuration (#7516)

## v0.4.0

:new: What's new:

- Return revert commit from Branch.revert() instead of None (#7353)
  **Deprecation:** use `reference` argument instead of `reference_id` (will be removed in the next major version)
- Allow kwargs in Branch commit creation (#7459)

:bug: Bugs fixed:

- Fix: Unconditionally assign body in ServerException constructor (#7315)

## v0.3.0

:new: What's new:

- Implement Cherry Pick (#7348)

## v0.2.1

:bug: Bugs fixed:

- fileno to throw exception (#7238)
- Transactions: Fix and add flag for branch cleanup (#7227)

## v0.2.0

:new: What's new:

- Allow creating reference types using Commit object (#7190)
- Implement Transactions (#7202)

:bug: Bugs fixed:

- Fix object access of uninitialized client (#7196)
- Handling file buffers on exceptions (#7195)

## v0.1.2

:bug: Bugs fixed:

- Fix urllib3 dependency (#7170)

## v0.1.1

:new: What's new:

- Support reader seek from end (#7147)

## v0.1.0

:new: What's new:

- First official release!
