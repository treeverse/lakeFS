# Change Log

## v0.8.3 / 2023-10-09
### Fixed
- package.json main property.

## v0.8.2 / 2023-10-09
### Remove
- package.json module property.

## v0.8.1 / 2023-10-09
### Added
- TypeScript base64 interfaces.
- Disable webpack polyfill.

## v0.8.0 / 2023-09-27
### Added
- TypeScript interfaces.
- HMAC feature.

### Fixed
- deprecated `new Buffer`, replace with `Buffer.from`.
- dependencies and security issues.
- don't modify global Array and ArrayBuffer prototypes.
- refactor: simplify formatMessage internal logic.

### Changed
- remove `eval` and use `require` directly.
- throw error by Error oject.
- throw error if update after finalize
- use unsigned right shift.

## v0.7.3 / 2017-12-18
### Fixed
- incorrect result when first bit is 1 of bytes. #18

## v0.7.2 / 2017-10-31
### Improved
- performance of hBytes increment.

## v0.7.1 / 2017-10-29
### Fixed
- incorrect result when file size >= 4G.

## v0.7.0 / 2017-10-29
### Fixed
- incorrect result when file size >= 512M.

## v0.6.1 / 2017-10-07
### Fixed
- ArrayBuffer.isView issue in IE10.

### Improved
- performance of input check.

## v0.6.0 / 2017-07-28
### Added
- support base64 string output.

## v0.5.0 / 2017-07-14
### Added
- support for web worker. #11

### Changed
- throw error if input type is incorrect.
- prevent webpack to require dependencies.

## v0.4.2 / 2017-01-18
### Fixed
- `root` is undefined in some special environment. #7

## v0.4.1 / 2016-03-31
### Removed
- length detection in node.js.
### Deprecated
- `buffer` and replace by `arrayBuffer`.

## v0.4.0 / 2015-12-28
### Added
- support for update hash.
- support for bytes array output.
- support for ArrayBuffer output.
- support for AMD.

## v0.3.0 / 2015-03-07
### Added
- support byte Array, Uint8Array and ArrayBuffer input.

## v0.2.2 / 2015-02-01
### Fixed
- bug when special length.
### Improve
- performance for node.js.

## v0.2.1 / 2015-01-13
### Improve
- performance.

## v0.2.0 / 2015-01-12
### Removed
- ascii parameter.
### Improve
- performance.

## v0.1.4 / 2015-01-11
### Improve
- performance.
### Added
- test cases.

## v0.1.3 / 2015-01-05
### Added
- bower package.
- travis.
- coveralls.
### Improved
- performance.
### Fixed
- JSHint warnings.

## v0.1.2 / 2014-07-27
### Fixed
- accents bug

## v0.1.1 / 2014-01-05
### Changed
- license

## v0.1.0 / 2014-01-04
### Added
- initial release
