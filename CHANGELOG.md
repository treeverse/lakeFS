# Changelog

# v1.64.1

:bug: Bugs fixed:

- Fix race while update branch returned from ref manager branch update (#9360)
- WebUI: Fix loading state when fetching repository data (#9366)
- API: update API spec from status code 423 (locked) to 429 (too many requests) (#9367)

# v1.64.0

:new: What's new:

- lakectl configurable symlink support (#9212)

:bug: Bugs fixed:

- Fixed the image overflow in the diff view in the UI (#9346)
- Allow trailing slash in lakectl fs upload (#9337)
- Log failed audit check in debug level (#9348)

# v1.63.0

:new: What's new:

- Implement Pre/Post Cherry-Pick Hooks (#9263)
- Support multipart for lakectl presigned uploads (#9283)
- Add gauge metrics for API, Gateway and Blockstore endpoints (#9260)

# v1.62.0

:new: What's new:

- Basic plugin support for lakectl (#9242)
- UI: Image content diff view (#9246)
- UI: Improved empty states for actions, tags (#9230)
- Support right object info in commit diff (#9248)

:bug: Bugs fixed:

- Clean temp cache files (#9250)
- UI: Fix some dark mode elements (#9237)

# v1.61.0

:new: What's new:

- UI: Unify uncommitted changes tab into objects (#9181)
- lakectl: Add retries to sync download (#9191)
- lakectl log: Support all refs  (#9170)

:bug: Bugs fixed:

- Fix web ui action menu dropdown position (#9198)

# v1.60.0

:new: What's new:

- Actions: Distinguish client and internal errors in actions responses (#9162)
- Update kin-openapi package with changes related to request validation (#9167)
- Sample repository: remove unused images (#9156)

:bug: Bugs fixed:

- Fix: laktectl progress bar panic (#9148)
- Fix issue with not showing marker icons in GeoJSON preview map (#9140)

# v1.59.0

:new: What's new:

- UI: Overhaul lakeFS look & feel (#9022)

:bug: Bugs fixed:

- Fix lakefs superuser command using external authorization (#9127)
- Fix missing BadRequest(400) in the API spec for get/head object (#9130)

# v1.58.0

🏢 Includes only changes for Enterprise.

# v1.57.0

:new: What's new:

- UI: turn off hive partitioning while select parquet using DuckDB (#9056)
- Add friendly name to GetUser API response (#9065)

# v1.56.1

:bug: Bugs fixed:

- Fix ParseRange to fail on bytes=-0 (#9032)
- Fix S3 ListMultipartUploads response missing NextUploadIDMarker (#9036)
- Use MD5 as ETag in gs adapter (#9041)
- lakectl - add AWS IAM login config (ignored) (#8994)

# v1.56.0

:new: What's new:

- UI : Add GeoJSON preview support (#8996,#9014)
- S3 Gateway: Add support for list parts for Google storage adapter (#9019)
- S3 Gateway: SigV4 Support Unsigned Payload Trailers (#9009)
- UI: Enable expand/collapse functionality for directory trees  (#8978)

:bug: Bugs fixed:

-  UI: Fix repository tree layout and link behavior (#8999)

# v1.55.0

:new: What's new:

- Support list repositories and branches in lakectl by prefixes by @AliRamberg in #8860
- Add pagination to Add Group Members and Attach Policies by @Ben-El in #8953
- Security fix - Replace old go-jwt by @Isan-Rivkin in #8968
- Support separate per-part requests for MPU presigned URLs by @arielshaqed in #8962
- lua delta exporter- add a function to find changed tables by @nadavsteindler in #8957
- Enable expand/collapse functionality for directory trees by @Ben-El in #8978

:bug: Bugs fixed:

- Fix - long branch name overflows in the UI by @Annaseli in #8948
- Fix - No prompt after clicking the button in access control page #8610 by @VH992098059 in #8932
- Fix - Parse configuration from environment slice of any by @nopcoder in #8980

# v1.54.0

:new: What's new:
- Support multiple resources policies (#8823)
- Presigned URL support in original filename content disposition (#8897)

:bug: Bugs fixed:
- Fix webui dropzone should be disabled while upload in progress (#8892)
- Import: prohibit import from storage namespace (#8917)
- Fix: Truncate Long Path Values in Upload Object Modal (#8920)
- Truncated the display of overly long branch names in the dropdown. (#8922)

# v1.53.1

:new: What's new:
- Support configuration populate arrays from environment variables (#8891)

:bug: Bugs fixed:
- Fix BI metadata collection (#8901)

# v1.53.0

:new: What's new:
- Added a lakectl flag to point to configurations file: `LAKECTL_CONFIG_FILE` (#8831)
- Added markdown files diff display to the webui (#8855)

:bug: Bugs fixed:
- Fixed: Use new Delta Lake client registration flow to handle multiple parquet readers (#8869)
- Fixed: Preserve PR form state when switching branches (#8870)

# v1.52.0

:new: What's new:
- Added Lua Action support for `update_object_user_metadata` (#8771)
- Hooks: Enhanced Lua `stat_object` to get metadata (#8777)
- Added search functionality for Groups and Policies in Web UI (#8783)
- Added search functionality for Users in Web UI (#8784)
- Actions: Added pre-post revert hooks (#8804)
- Actions: Added prepare-commit hook (#8788)

:bug: Bugs fixed:

- Fixed Web UI Merge Dialog - Enter Should Submit (#8781)
- Fixed Web UI Auth Error after first login (#8806)
- Fixed: Revert with allow empty commit (#8803)
- Fixed: Create bare repository now skips storage namespace check (#8816)
- Fixed lakeFS Lua package to properly post data in request (#8768)

# v1.51.0

:new: What's new:
- Hook pre/post merge include merge source (#8703)
- Switch to use openapi generator v7.0.1.2 (#8710)
- lakectl: Add storage ID to repo list (#8756)

:bug: Bugs fixed:
- WebUI: Fix overflowed values in modals (#8640)
- WebUI: Fix overflowed policy name error (#8705)

# v1.50.0

:new: What's new:
- Accelerate large files download using concurent chunks for lakectl download recursive (#8613)
- lakectl support create repository with sample data (#8628)
- Enable parse config map properties from environment string value (#8657)
- Upgrade code to use openapi-generator-cli v7.0.1.1 (#8652)
- Renewed the PGP key (#8661)
- Add support for no-check flag in setup command (#8591)

:bug: Bugs fixed:
- UI: Truncated long value names in selected in the AttachModal (#8668)
- UI: Truncated long value names in groups, users and policies (#8666)
- UI: Truncated long value names in confirmation window (#8670)

# v1.49.1

:bug: Bugs fixed:
- Fix: UI Create user (#8604)
- Fix: Change validation error message on create repository to reflect validation (#8585)

# v1.49.0

:new: What's new:
- change login placeholders for remote authenticator (#8519)
- Make http client type configurable (#8516)
- S3GW: Support list multipart uploads (#8531)
- Metric: Report time-to-first-byte for get object API (#8503)

:bug: Bugs fixed:
- Fix: react-dropzone dependency (#8534)
- Add admin/superuser error handling and cleanup (#8559)


# v1.48.2

:bug: Bugs fixed:
- added storage namespace validation on non-readonly bare repo creation (#8364)

# v1.48.1

:bug: Bugs fixed:
-  Squash merges is ON by default #8482 (switched to OFF)

   This bug **breaks** backwards compatibility by causing all merged to be
   squashed by default.

# v1.48.0

## :warning: Do **NOT** use version 1.48.0 :warning:

It squashes merges by default, which is incorrect and a breaking change (#8482)

:new: What's new:
- Upgrade to Golang 1.23 (#8452)
- Add "squash merge" support to merge API (#8464)

# v1.47.0

:new: What's new:
- S3 GW: Support PutIfAbsent (#8428)

# v1.46.0

:new: What's new:
- Feature: Repository Search by Substring (#8417)

# v1.45.0

**Note:**
The legacy Python client has been discontinued as of this release. Its development, distribution, and codebase have been terminated.

:new: What's new:
- Remove Python Legacy Client (#8410)
- Add friendly name to list group members response (#8413)

# v1.44.0

**Note:**
The legacy Java client has been discontinued as of this release. Its development, distribution, and codebase have been terminated.

:new: What's new:
- Support for hidden branches (#8375)

:bug: Bugs fixed:
- Fix: Auth no endpoint error (#8407)
- Fix: Delta Exporter- Handle vacuumed objects correctly (#8409)
- Fix: Allow pre-signed upload via the web UI (#8365)

# v1.43.0

:new: What's new:
- Support Dark Mode for Text Viewer (#8371)

:bug: Bugs fixed:
- Fix: lakectl FS upload big files cause OOM (#8349)
- Fix: CosmosDB panic in handleBatchSizeChange(#8367)
- Fix: RepoManagementReadAll pull requests permissions (#8374)

# v1.42.0

:new: What's new:
- Optionally set mtime in linkPhysicalAddress (#8338)

:bug: Bugs fixed:
- Fix: EnsureStorageNamespace close reader (#8346)
- Fix: Git package error handling (#8345)

# v1.41.0

:new: What's new:
- command to check current user in lakectl (#8322)

:bug: Bugs fixed:
- Use time of create MPU on backend storage as time of MPU (#8311)
- Fix: Pull Requests merge errors (#8302)
- Fix "http: read on closed response body" error (#8332)
- Fix: Get mtime from storage server (#8329)

# v1.40.0

:new: What's new:
- Added Parallelism configuration option to lakectl (#8283)
- Experiment: Improve concurrent merge performance by weakly owning branch updates (#8268)
- Add documentation for standalone (sparkless) GC (#8307)

:bug: Bugs fixed:
- Pass request context to import operation sub tasks (#8320)

# v1.39.2

:new: What's new:
- lakectl log: add option to filter merge commits (#8142)
- Add a deprecation and upgrade warning to python-legacy client (#8195)
- Make update object metadata API atomic (#8264)

:bug: Bugs fixed:
- Fix: GC: Read commit explicitly when it is missing from prescanned commits (#8282)
- Fix: Copy object mtime (#8291)

# v1.38.0

:new: What's new:
- WebUI: support jsonl/ndjson file diff (#8136)
- Separate hidden and non-hidden commands in lakectl docs (#8204)

# v1.37.0

:new: What's new:
- Experimental: add update object metadata API (#8253)

:bug: Bugs fixed:
- Fix Sample Data checkbox of the Repository Create form (#8255)
- Add Effect to PR statement (#8254)

# v1.36.0

:new: What's new:
- Pull Requests for Data (#8235)

:bug: Bugs fixed:
- lakectl local checkout src uri (#8213)

# v1.35.0

:new: What's new:
- AWS Pre-signed url endpoint configuration (#8170)

# v1.34.0

:new: What's new:
- WebUI Dark Mode (#8137)

:bug: Bugs fixed:
- Make Blockstore Type config case insensitive (#8146)

# v1.33.0

**Note:**
PLEASE READ CAREFULLY BEFORE UPGRADING TO THIS VERSION!!!
This version removes the built-in ACLs functionality, and might require some users to take action.
Refer to [documentation](https://docs.lakefs.io/security/access-control-lists.html) for more information.

:new: What's new:
- ACLs: Remove server usage (#8126)
- S3 Gateway CopyObject repsect metadata directive header (#8117)

# v1.32.1

:bug: Bugs fixed:
- lakectl local: fix dir seek (#8105)

# v1.32.0

:new: What's new:

- lakectl: add --no-progress flag for disabling progress bar animation (#8093)

# v1.31.1

:bug: Bugs fixed:
- Handle empty page on CosmosDB Iterator (#8066)

# v1.31.0

:new: What's new:
- lakectl local: add option to ignore symlinks (#8055)
- Allow Glue exporter to create a database per branch (#8000)

# v1.30.0

:new: What's new:
- KV Scan optimization - paginate with exponential jumps (#8002)

# v1.29.0

:bug: Bugs fixed:
- GCS Multipart Upload Performance Fix (#7822) (#7953)
- Fail `revert` operation in the Python wrapper in case both `reference` and `reference_id` args are provided (#7983)
- Show deprecation warnings from the Python wrapper by default (#7987)
- Fix lakectl upload from standard input (#7984)
- Resolve panic caused by nil parameter in enhanceWithFriendlyName function (#7989)

# v1.28.2

:bug: Bugs fixed:
- Spark client panics when reading DataFrame from entire repository / storageNamespace (#7955)

# v1.28.1

:new: What's new:

- Metric for open connections (#7913)

# v1.28.0

:new: What's new:
- Add `flare` command (#7854)

:bug: Bugs fixed:
- Enforce MonitoredAuthService to implement EmailInviter when required (#7916)

# v1.27.0

:bug: Bugs fixed:
- CosmosDB iterator to use dynamic batch size after the first list (#7892)
- Fix S3 bucket validation for inter-region storage (#7896)

# v1.26.1

:new: What's new:
- Introduce support of GCS encryption for both CMEK and CSEK (#7809)
- Support prefix parameter in lakectl diff command (#7832)
- Enforce same-region storage for new repositories (#7847)
- Add metrics to auth (#7840)
- Allow changing commit data in cherry-pick and revert (#7865)
- Prevent unnecessary operations on write actions to read-only repository in s3 gateway (#7844)
- Return an informative error message when using 'api/v1' for S3 GW endpoint (#7828)

:bug: Bugs fixed:
- Make batch_size in kv.Scan effective for first Scan of iterator (#7875)
- Fix lakectl local diff slowness (#7842)

# v1.25.0

:new: What's new:
- Add OS info to `lakectl` User Agent (#7759)
- Serve GZipped Static Assets (#7788)
- Propagate request ID from context to API auth service calls (#7803)
- Add more context-based logging (#7804)
- Add "allow-empty" flag to Merge (#7798)
- Sync Manager to use HTTP client with retries (#7815)

:bug: Bugs fixed:
- `lakectl local commit`: Prevent remote changes outside synced prefix (#7796)
- UI: Restore colors in diff view (#7808)

# v1.24.0

:new: What's new:
- Make MaxBatchDelay configurable (#7774)
- Handle empty commit messages (#7767)
  * 🐣 Welcome @itaigilo!

# v1.23.0

:new: What's new:
- Support presigned image URLs in markdown (#7745)
- Make the number of DynamoDB connections configurable (#7751)

:bug: Bugs fixed:

- Fix background staging token behaviour (#7754)
- Fix: pre-merge is missing commit ID (#7750)

# v1.22.0

:new: What's new:

- UI: Report progress when calculating size (#7722)
- Handle KV slow down error (#7744)
- Remove linked addresses from KV (#7725)
**Upgrading to this version will void all existing physical addresses issued by GetPhysicalAddress and require them to be re-issued. This will only effect some of the in-flight uploads during the upgrade**

:bug: Bugs fixed:

- Fix: Object diff to use pre-sign when presign ui is enabled (#7736)

# v1.21.0

:new: What's new:

- Warn when creating lakectl local on case-insensitive filesystems (#7650)
- lakectl added support for retries (#7723)
- S3 gateway - return slowdown on DynamoDB throttle (#7685)
- UI: calculate total size of common prefix (#7720)

:bug: Bugs fixed:

- DDB temp creds to refresh before expiry (#7718)
- Close resp body in lua s3 client (#7703)

# v1.20.0

:new:  What's new:

- Python SDK: Support multiple pydantic versions (#7672)

# v1.19.0

:new:  What's new:

- Experimental: Support pre-signed redirects for S3A (#7630)
- lakeFSFS: IAM Role Support Login call (#7659)
- Azure: Add support for Gov Cloud (#7664)
- Revert "allow lakectl local to be 'git data' (#7618)" (#7654)

:bug: Bugs fixed:

- Fix: S3 GW ListParts (#7629)

# v1.18.0
:new: What's new:

- LakeFSFS - TokenProvider AWS (no login yet) (#7604)
- Add AWS remote auth login (#7578)

# v1.17.0
:new:  What's new:

- Allow lakectl local to be "git data" (#7618)
- Support STS in Python wrapper (#7620)

:bug: Bugs fixed: 
- Fix storage config loading state not updated (#7638)
- Include hidden files in `lakectl local` (#7644)
- Use username as token subject in STS login (#7637)

# v1.16.0
:new: What’s new:

- lakectl: get presigned url for object (#7603)
- Align S3 Gateway Prometheus Histogram Buckets with lakeFS API Buckets (#7622)

:bug: Bugs fixed:

- Allow storing of friendly_name in KV when integrating with external auth API (#7572)
- Add TTL to STS (#7605)

# v1.15.0

**Deprecation notice:**
Azure users no longer need to use the import `hint` when importing from an ADLS Gen2 account.
We've added a backwards compatibility support in lakectl, but support for the ADLS `hint` will be removed completely in future releases.

:new: What's new:

- Integrate support for Short-Lived Tokens (STS) in Remote Authentication (#7571)

:bug: Bugs fixed:

- Fix lakectl branch protection set 400 response (#7590)
- Remove ADLS hint from import (#7581)

# v1.14.1

:new: What's new:

- Integration: update experimental external principals API with auth service (#7566)

:bug: Bugs fixed:

- Fix lakectl local diff (#7563)

# v1.14.0

:new: What's new:

- Delta exporter: support export of delta log files with abfss scheme to support unity catalog external tables (#7553)
- Auth Service: External Principals Management endpoints only (#7539)

:bug: Bugs fixed:

- Support Azure Unity Catalog export (#7554)
- Groups Pagination is broken in the WebUI (#7556)
 
# v1.13.0

:new: What's new:
- Delta Lake + Unity Catalog exporter: Delta Lake table metadata (#7527)
- Handle Azure URLs for chinacloud (#7520)
- Support "If-None-Match: <etag>" header in getObject request (#5967).
  Kudos to our latest contributor @vishalsanfran!

:bug: Bugs fixed:
- Fix: Count KV retries (not tries) in Graveler (#7541)
- Fix: CreateDirectoryMarkerIfNotExists (#7510)
- Fix: Fixed bug in multipart upload s3 (#7512)
- Fix: Gateway wrong error description for copy object (#7526)

# v1.12.1

:bug: Bugs fixed:

* Check for correct RBAC action (GHSA-fvv5-h29g-f6w5).  This security
  advisory has **moderate** severity (5.3/10), and does not affect users who
  use the default ACL permissions.

# v1.12.0

:new: What's new:
- Add If-None-Match to LinkPhysicalAddress (#7480)
  **Removes deprecation of If-Non-Match from upload object!**
- Skip actions for read-only repositories (#7477)
- [hooks] Log the user when fetching Lua script from lakeFS (#7486)

# v1.11.1

:new: What's new:
- Disable GC and branch protection for read-only repositories (#7471)

:bug: Bugs fixed:
- Fix S3 gateway cross-repo copies (#7468)
- Delta export path unescape (#7473)

# v1.11.0

:new: What's new:

- Delta Exporter: Azure Support (#7444)
- Prohibit empty revert by default (#7308)

:bug: Bugs fixed:

- Fix: Skip ensureStorageNamespace when creating a read-only repository (#7449)
- Fix: Show email or group name in place of ID in AttachModal (#7454)
- Fix: Show email when available in the create credentials confirmation modal (#7456)
- Fix: bug on creating bare repository as read-only (#7458)

# v1.10.0

:new: What's new:

- Remove Delta Diff experimental support + code cleanup (#7343)

:bug: Bugs fixed:

- Handle empty metadata on import (#7363)
- Remove unnecessary CreateBranch permission check on import (#7362)

# v1.9.1

 :bug: Bugs fixed:

- API: make version and generation fields optional for commit- fix a breaking change introduced in v1.9.0 (#7323)

# v1.9.0

:new: What's new:

- lakectl: download single file using presign now support multipart (#7284)
- API: separate group id and display name (#7292)

**Note:** the separation of group IDs from displayNames isn't a breaking change.
However, if using older generated clients with the latest lakeFS version,
IDs will be displayed instead of the human-readable displayNames when using an external authentication API.

# v1.8.0

:new: What's new:

- Add "hard reset" operation (#7263)
- Usage report for API and S3 gateway (#7281)

:bug: Bugs fixed:

- Fix docker image build for cross compile (#7286)


# v1.7.0

:new: What's new:

- API: Presigned multipart uploads for S3. You can initiate presigned multipart uploads for Amazon S3 directly through the API. **experimental** (#7246)

:bug: Bugs fixed:

- UI: DuckDB cached files fixed with cache buster: No more worries about outdated files. (#7252)


# v1.6.0

:new: What's new:

- Allow empty commits option #7186

# v1.5.0

:new: What's new:

- Read-only repositories (#7157)
- Add StopAt field to the CommitLog action (early-stop at a specific commit) (#7222)

:bug: Bugs fixed:

- Fix: Return "bad request - 400" on reset branch with unknown type  (#7210)
- Fix: S3-gateway "delete" operation with branch protection should return 200 with error (#7211)

# v1.4.0

:new: What's new:

- Delta Lake Catalog Exporter (#7078)
- Unity Catalog Exporter (#7167)

# v1.3.1

:bug: Bugs fixed:

- Fix: User with permission to write actions can impersonate another user when auth token is configured in environment variable
  ([GHSA-26hr-q2wp-rvc5](https://github.com/treeverse/lakeFS/security/advisories/GHSA-26hr-q2wp-rvc5))
- Fix: S3 Gateway block unsupported S3 operations (#7028)
- Fix: Better error handling on hook error (#7081)
- Fix: Upload object without specify content type (#7130)
- Fix: UI notebook preview fix colors (#7141)
- Fix: Match blockstore reader hash function (#7099) (thanks @hunjixin)
- UI improve load by cache and split of embedded content (#7132,#7135)

# v1.3.0

:new: What's new:

- Update AWS Go SDK v2 to support S3 express directory buckets (#7083)
- API and lakectl: support long-running dump and restore operations (#6975)
- lakectl: Improved performance for fs rm recursive command (#7035)
- Glue Exporter: Support Hadoop directory markers (#7058)

:bug: Bugs fixed:

- API: Fix StatsObject returned metadata on empty metadata (#7026)
- API: Return the right default pagination per page- 100 instead of 1000 (#7051)
- DynamoDB KV: Check if a table exists before we try to create it (#7056)

# v1.2.0

:new: What's new:

- UI: Allow commit message and metadata in merge action (#6897)
- API: CreateRepository check if repository exists before trying to validate storage namespace (#6967)
- lakectl: Add commit params to merge command (#6892)
- Optimization - Reduce DynamoDB and CosmosDB calls by extending current range to include additional information (#6983)

:bug: Bugs fixed:

- Fix - Lakefs crashing when using database.type=”dynamodb” and scylladb as a database (#6924)
- Change 'committer' field from UserID to Email, when Email is available (#6912)
- Use Buf CLI to compile our proto files (#6784)
- Log time.Duration _twice_ in all modes, as string and as nanoseconds (#6934)
- External auth remove required id from user model (#6902)

# v1.1.0

:new: What's new:

- API: Added optional since parameter to commit log request (#6851)
- lakectl: supported log 'since' flag (#6854)
- Improved lakectl validation error messages (#6816)

:bug: Bugs fixed:

- Fixed S3 gateway error on no underlying object (#6822)
- Fixed branch protection 'not found' error (#6846)

# v1.0.0

:new: What's new:

After more than 3 years since its initial public release...

🎉 **lakeFS 1.0 is now generally available** 🎉

This is a huge milestone for the lakeFS maintainers, contributors and community and we can't be more excited!

## Benefits

1.0 is more than just a cosmetic change - it includes a few important benefits and guarantees to anyone running or using lakeFS:

* lakeFS 1.x ensures both [backwards]() and [forwards]() compatibility with any future 1.x release, as per [Semver](https://semver.org/).
* Not just the [lakeFS Open API](https://docs.lakefs.io/reference/api.html): This is also true for the [new lakeFS Python SDK](https://docs.lakefs.io/integrations/python.html#using-the-lakefs-sdk), the [Java SDK](https://central.sonatype.com/artifact/io.lakefs/sdk?smo=true) and the [lakectl CLI](https://docs.lakefs.io/reference/cli.html) tool.
* Bug fixes will be released for lakeFS 1.x even after the release of future major version, and at least until October 2024
* Security fixes will be released (and if needed, backported) even after the release of future major version, and at least until June 2025(!).
* While lakeFS is already trusted in production in hundreds of organizations around the world, 1.x ensures a stable API going forward and it is highly recommended for all lakeFS users.

## Feature Highlights

The lakeFS 1.0 release actually doesn't introduce any new capabilities other than the guarantees given above. However,
for users of slightly older 0.x lakeFS versions, here are a few highlights from the last few months that are included with this version:

- `lakectl local` - Allows syncing a local directory with remote data in lakeFS, including full Git integration ([read announcement](https://lakefs.io/blog/scalable-data-version-control-getting-the-best-of-both-worlds-with-lakefs/))
- **iceberg catalog** - Making it possible to branch, commit and merge multiple Iceberg tables together and in tandem with other formats ([read announcement](https://lakefs.io/blog/using-lakefs-with-apache-iceberg/))
- **lua based hooks** - Run your own custom logic when commits, merges and other events occur. No need to host a webhook server or deploy anything! ([read announcement](https://lakefs.io/blog/introducing-lua-hooks/))
- **enhanced security with pre-signed URLs** - Allows lakeFS to version data it cannot even read! This enhanced security by leveraging pre-signed URLs supported on all common object stores, to allow lakeFS to authorize access to data without having to proxy that data through the lakeFS server ([read more](https://docs.lakefs.io/reference/security/presigned-url.html))
- **[Many many](https://github.com/treeverse/lakeFS/pulls?q=is%3Apr+is%3Aclosed) bug fixes and performance improvements across the board** 

## Notes on upgrading from lakeFS 0.x

While most of the API hasn't changed much, there are a few notable API changes between 0.x and 1.x.

The migration path is usually pretty simple and not many code changes are required - but *please read through the list of breaking changes*:

[lakeFS 1.0 - Code Migration Guide](https://docs.lakefs.io/project/code-migrate-1.0-sdk.html)

For more information on the benefits of upgrading to 1.0 and the migration path, please refer to the [lakeFS documentation](https://docs.lakefs.io/understand/towards-1.0-sdk.html) or [reach out on Slack](https://lakefs.io/slack).

:bug: Bugs fixed:

- Increased the maximum attempts for DynamoDB with configuration support (#6804)
- Amended Azure import to utilize content MD5 instead of entry Etag (#6802)
- Corrected issue where the UI upload failed due to conversion of undefined or null values (#6818)

# v0.113.0

:new: What's new:

- Reintroduce recursive flag for lakectl fs download/upload (#6777)
- Spark client update to use the latest lakefs SDK package (#6742)

:bug: Bugs fixed:

- Fix lakectl upload with pre-sign for Azure (#6660)
- Fix lakeFSFS ETag value when using pre-signed mode (#6751)
- Fix LinkPhysicalAddress trim quotes and spaces from checksum (#6749)
- Fix lakectl upload using pre-signed use ContentMD5 header for ETag (#6750)
- Fix LinkPhysicalAddress response with full path (#6748)
- Fix lakectl download error with bad path traversal blocked (#6775)
- Fix UI Azure pre-sign upload (#6764)
- Fix UI upload azure pre-signed URL checksum as hex md5 (#6770)
- Fix UI copy pre-signed URL not working when pre-sign UI disabled (#6776)
- Fix delete branch protection prevent commit because invalid state (#6788)


# v0.112.0

:new: What's new:
- New Java SDK (#6656)
- New Pyhon SDK (#6562)
- Export Hooks: Glue Catalog exporter (#6653)
- Remove recursive flag from lakectl fs Upload and Download (#6681)
- CosmosDB: Add throughput configuration (#6693)
- lakectl: block `fs stage` command for objects in the storage namespace (#6696)
- API: Deprecate stageObject (#6690)
- API: linkPhysicalAddress do not verify external addresses (#6667)
- API: GC prepare move operations to internal tag (#6700)
- API: support response Too many requests 420 (#6711)
- API: deprecate upload object If-None-Match and Storage Class (#6708)
- LakeFSFileSystem is now compatible only with lakeFS versions >= 0.108.0 (#6725)

# v0.111.1

:bug: Bugs fixed:
- Fix email display bug when using external auth provider (#6680)
- Fix repository creation bug in quickstart mode (#6682)

# v0.111.0

:new: What's new:
- lakectl endpoint configure and endpoint path reuse (#6609)
- Remove UpdateBranchToken API (#6590)
- Table Extractor Hook and _lakefs_tables format  (#6589)
- Export Hooks: Symlink Exporter (#6636)
- Move setting endpoints to be under `/settings/` (#6649)
- Remove expired commits from prepare_gc_commits response (#6634)
- Remove lakectl dbt deprecated functionality (#6632)
- Remove lakectl direct deprecated functionality (#6623)
- API cleanup: remove emailer service implementation (#6661)
- API cleanup: consolidate lakefs level information API (#6647)
- API cleanup: make createSymlinkFile internal (#6630)
- API cleanup: Mark refs dump/restore current API as internal (#6640)

:bug: Bug fixed:
- Fix: lua s3 client endpoint configure if set (#6629)
- Fix: UI alert if "quickstart" repository failed on local (#6622)
- Fix: Lua runtime - marshaling arrays (#6655)
- Fix: Presigned URL Cache - change to Default expiry due to bug in package (#6651)

# v0.110.0

:new: What's new:
- Upgrade code to use AWS SDK go v2 (#6486)
- Actions: Lua package to parse URL (#6597)
- UI: Commit info consistent across screens (#6593)
- API: Update APIs tag for internal use (#6582)
- lakectl: dbt and metastore commands marked as deprecated (#6565)
- Removed deprecated: expand template API (#6587)
- Removed deprecated: remove the update and forgot password APIs (#6591)

:bug: Bug fixed:
- Fix lakectl local verify bad path error on Windows (#6602)
- Fix Azure pre-signed URL for blob imported from different account (#6594)
  
# v0.109.0

:new: What's new:
- Restrict lakectl local to common prefixes only (#6510)
- lakectl local gitignore end marker (#6522)
- Remove logBranchCommits (#6528)
- Lua package for yaml support (#6545)
- lakectl skip stats for unrelased version (#6551)
- Remove OTF diff APIs from non experimental tag/category (#6563)
- S3 API: Propogate slowdown response from cosmosdb to client (#6556)
- logging and lakectl NO_COLOR support (#6569)
 
:bug: Bug fixed:
- Fix: lakectl import with Azure Data Lake Storage (#6515)
- Fix: UI pdf viewer by X-Frame-Options set to SAMEORIGIN (#6520)
- Fix: Lua gs client close (#6547)
- Fix: Lua path.Join to handle concatenation of two separators (#6549)
- Fix latest version cache and remove the use of go-github (#6572)
- Fix: DynamoDB SeekGE bug (#6561)

# v0.108.0

:new: What's new:
- Support vertex AI (Datasets + Fuse) (#6492)
- A better way to handle interrupts in lakectl local (#6455) 
- When creating Repository: `dummy` file location changed from `<storage-namespace>/dummy` to `<storage-namespace>/_lakefs/dummy` (#6497)
- Removed support for migration from lakeFS version < v0.50.0 (#6497)
- Deprecate lakectl fs "--direct" flag (#6480)

:bug: Bug fixed:
- Fix: Ensure that metadata is included in merge operations (#6500)
- Fix: Increase DynamoDB concurrent requests (#6489)
- Fix: UI ignores presigned URL configuration for GetObject requests (#6477)
- Fix: lakectl local sync parallelism (#6485)
- Fix: UI object information slides outside the modal dialog (#6501)
- Fix: CosmosDB SeekGE query from iterator start key (#6503)
- Fix: UI ADLS import validity regex (#6509)

# v0.107.1

:bug: Bug fixed:
- Fix: Allow disabling pre-signed URLs for google storage block adapter (#6462)
- Fix: lakectl local with windows (#6464)

# v0.107.0

:new: What's new:
- Experiment: Batch staging get operations for DBIO transaction markers (#6441)

:bug: Bug fixed:
- Fix kv local SeekGE fail to skip common prefix (#6435)
- Fix lakectl local commit to use lakefs upload with user metadata (#6437)

# v0.106.2

:bug: Bugs fixed:

- Refresh credentials if needed before pre-signing URLs, include S3 credentials expiry in their requested lifetime, and make these S3 parameters configurable (#6392)
- Add meaningful error on block unsupported operation and check pre-sign capabilities in lakectl local (#6408)

## v0.106.1

- Fix: quickstart validation (#6402)

## v0.106.0

:new: What's new:
- lakectl local release (#6388)
- Bump vite from 4.0.2 to 4.0.5 in /webui (#6026)
- Graveler: use result from memory on seek (#6339)

:bug: Bugs fixed:
- Fix: pass nil reader when content size is 0 (#6367)
- Fix cosmosdb pagination bug (#6336)
- Add a warning in lakectl when catalog id is empty (#6381)
- Arbitrary JavaScript Injection via Direct Link to HTML Files ([GHSA-9phh-r37v-34wh](https://github.com/treeverse/lakeFS/security/advisories/GHSA-9phh-r37v-34wh))

## v0.105.0

:new: What's new:
- Support .text as a text file extension in the Web UI (#6130)
- UI: allow loading >10k diff summary (#6287)
- Add `diff_branch` to Lua hook (#6318)
- Setup lakefs on startup using predefined setup parameters (#5962)
- Quickstart now uses DuckDB WASM instead of CLI (#6092)
- Stop publishing lakeFS DuckDB image (#6141)
- Remove Everything Bagel - incorporated into the new lakeFS-samples repository (#6146)
- Return presigned URL expiry time (S3 only for now) (#6328)

:bug: Bugs fixed:
- Local Storage Range Allow 0:0 (#6116)
- Fix: Create repo with invalid namespace (#6084)
- Fix broken link to WAP notebook (#6121)
- Fix create and delete branch protection error and lakectl output (#6135)
- Import should replace the destination prefix with the source's state (#6251)

## v0.104.0

:new: What's new:
- UI: Support uploading multiple files and directories (#6074)

:bug: Bugs fixed:
- Delta Diff fails on certain operations (#6065)
- Fix: s3 gateway returning 200 instead of 206 for range requests (#6101)
- DuckDB: handle line breaks after a string constant (#6109)
- Add relative ./ path support for MD images (#6057)
- Fix pre-signed URL UI config default and property name - Azure and GCS (#6073)

## v0.103.0

:new: What's new:
- Add Azure Cosmos DB kvstore driver (#5915)
- UI: Allow writing to lakeFS from duckdb-wasm (#6044)
- Show object's user metadata in UI and CLI (#6050)

:bug: Bugs fixed:
- Fix gateway list bucket 1k limit (#6025)
- Fix lakeFSFS non simple access mode with address translator (#6028)
- Fix: Migration start version (#6039)

## v0.102.2

:bug: Bugs fixed:
- Using not owned LazyValue after closing a reader (#6004)
- Fix pre-signed URL UI config default and property name (#6014)

## v0.102.1

:bug: Bugs fixed:
- Non-Blocking write to wakeup channel (#6007)

## v0.102.0

:new: What's new: 
- Onboarding progress indicator - repository level (#5876)

:bug: Bugs fixed:
- Publish docker lakefs+duckdb with latest tag (#5993)
- Fix import start permissions (#5996)

## v0.101.1

**This release eliminates the need to go through previous ACL migration versions (v0.98.0 and up)**
Migrations can be perfomed from this version instead

:new: What's new: 
- Improved Import experience
- UGC prepare configurable max file size and produce time (#5969)
- Migrations: Revert remove ACL migration (#5942)
- Merge API response - remove summary field (#5115)
- Split and publish lakefs duckdb build and images (#5985)

:bug: Bugs fixed:
- GC: Read provided mark ID's run id in case that the run is a sweep-only run (#5936)
- Avoid panics after errors in log-commits (#5956)
- Fix extended stats user id report (#5970)
- Fix branch truncation in object viewer (#5982)

## v0.101.0

**This release requires running database migration.**
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version.

:new: What's new: 
- Refactor import (#5840)
- Deprecate ingest command (#5882)
- Support TLS for lakeFS local development (#5322)
- Generate JWT while working with auth API when non is configured (#5894)
- Remove email authenticator middleware (#5884)

:bug: Bugs fixed:
- Fix UI rename Error control to AlertError fixing conflict with class (#5907)
- Fix loading metadata cache with path separator prefix (#5922)
- Fix database setup for non-external mode (#5860)

## v0.100.0

:new: What's new:
- Support `--first-parent` option for commit log (#5733)
- Homebrew support as part of lakefs release (#5738)
- [actions] Add conditional execution to hooks (#5707)
- :wind_face: [UI] Display buttons to open (Airflow) metadata from UI (#5763)
- [bagel] Add docker-compose-airflow (#5757)
- :gift: Add Sample Repository with Quickstart option for new repositories (#5787)

:bug: Bugs fixed:
- [API] LinkPhysicalAddress twice should fail with status code 400 (#5727)
- [UI] Add back the copy URI button + add cache-control header to GET objects (#5731)
- [export] Fix lakeFS export docker image to use python 3.11 (#5755)
- [S3gw] Fix s3 multipart upload abort (#5725)
- [setup] Fix warning on ACL write group for ACL during setupFS (#5792)
- [UI] Fix repository name while validating storage namespace (#5802)
- [UI/setup] Require admin username in initial setup (#5818)
- [lakectl] Validate fs upload path is not empty (#5819)
- [lakectl] refs-dump output should be valid JSON (#5831)
- [auth] Add metarange creation permissions to Developers and to Writers (#5833)
- [UI] only show group ACL column in simplified mode (#5843)
- [auth] Remove auth API pass and use username with credentials (#5838)

## v0.99.0

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version.
Please refer to this [upgrade documentation](https://docs.lakefs.io/reference/access-control-lists.html#migrating-from-the-previous-version-of-acls) for more information on the specific migration from RBAC to ACL

:new: What's new:
- Lua optional net http request (#5679)
- bisect lakectl support (#5381)
- Migrate up to ACL if there are no warning (#5657)
- Remove Repositories from ACL (#5680))
- Reduce listObjects calls and amount (#5683)
- Write expired addresses once (#5631)
- Add configuration for RBAC type in UI (#5656)
- [GC] Partition and aggregate expired addresses more efficiently (#5618)

- :bug: Bugs fixed:
- Fix priority queue used by merge find base (#5668)

## v0.98.0

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version.
Please refer to this [upgrade documentation](https://docs.lakefs.io/reference/access-control-lists.html#migrating-from-the-previous-version-of-acls) for more information on the specific migration from RBAC to ACL


:new: What's new:
- change authorization from RBAC to ACL (#5338)
- Notify new lakefs version in lakectl and Web UI (#5608)
- Added code highlighting for markdown code blocks + prettier formatting (#5590)
- Preview Build and Check links for any doc PRs (#5601)
- Bump Java version in SBT (#5615)
- Safer fs for config in HadoopFS (#5602)
- Added support for Azure service principal (#5549)

:bug: Bugs fixed:
- Fix adls import (#5625)

## v0.97.5

:bug: Bugs fixed:

- Fix: show GC rules missing warning not showing up (#5582)
- Fix: dotgraph commit urls are broken (#5583)
- Fix azure data lake v2 import (#5563)
- fix presign UI  (#5586)

## v0.97.4

:bug: Bugs fixed:

- Fix lakeFS clients - revert OpenAPI generator cli version (#5572)

## v0.97.3

:new: What's new:

- Delta Lake Tables diff - BETA: Compare Delta Lake tables between different refs - UI only (#5427)
- Cherry-pick a commit to a branch (#5483)
- Diff UX improvements (#5414)
- DuckDB/lakeFS improvements (#5430)
- Display images in markdown with support for lakefs:// URIs (#5449)
- DynamoDB KV error count without context cancellation (#5512)

:bug: Bugs fixed:

- Fix inconsistent marshaling in DynamoDB KV (#5409)
- Fix S3 accept header prefer application/xml (#5447)
- Fix lakectl table write to non TTY non string column (#5500)
- Fix S3 adapter access content length without nil check (#5499)
- Fix UI DuckDB fix re-query updated data (#5536)
- Fix object name wrapping in UI (#5532)
- Fix iterator panic on close after failed SeekGE (#5516)
- Remove stack trace from error message in the UI (#5523)

## v0.96.1

:bug: Bugs fixed:

- Fix registration form (#5420)

## v0.96.0

:new: What's new:

- lakectl: output commit log as dotgraph (#5397)
- Improve GC prepare-commits call performance (#5377)
- UI: Warn when no gc rules are defined for long living repos (#5400)

:bug: Bugs fixed:

- Fix inconsistent setup state (#5410)

## v0.95.0

:new: What's new:

- [Breaking Change] Added Remote Authenticator service to replace embedded LDAP authenticator (#5285)
- Verify authenticity of tokens provided by external IdPs (#5385)
- Support merge directly from the import wizard (#5352)
- Include source IP in logs (#5216)

:bug: Bugs fixed:

- Include refs for tag/branch in `GetCommit` response (#5369)
- Fix object URI wrapping for long URIs (#5274)

## v0.94.1

Bugs fixed:
- UI: fix import button disabled even though enabled by configuration (#5336) (#5337)
- Log actual error in debug when reporting it to an API call (#5327)

## v0.94.0

What's new:
- Local block adaptor import support (#5277)
- Add merge strategy to commit data (#5252)
- Configurable KV health check for DynamoDB (#5282)
- Configurable pre-signed URL support (#5308)

Bugs fixed:
- Fix: UI prefix search widget functionality (#5249)
- Fix: Branch protection return status code should return Forbidden(403) (#5273)

Deprecated:
- Authorization using Role-Based Access Control (#5233)
- LDAP Embedded Support (#5262)

## v0.93.0

:new: What's new:

- Export gob binary format registration to allow external services to use
  auth.Claims (#5184)
- Add copy URI button to objects navigator in web UI (#5185)
- Configure web UI login URLs and details dynamically (#5093)
- Enable configurable logout URL in the UI  (#5203)
- Add an exchange refs option to web UI compare view (#5200)

:bug: Bugs fixed:
- Fix broken "compare" link in web UI import wizard (#5189)
- KV postgres: use advisory lock when creating tables during initial setup (#5193)

## v0.92.0

:new: What's new

- Deprecate embedded support for OIDC (#5061)
- Update policies page with note about RBAC deprecation (#5150)
- Configurable pre-signed url expiry (#5144)
- Azure: Add support for async copy (#5118)
- Allow configuration of GUI warning message for deprecating RBAC (#5169)

:ladybug: Bug Fixes
- UI: fix lakectl configuration download format after setup (#5167)
- Measure email subscription drop rate (#5145)

## v0.91.0

:new: What's new
- Support lakectl pre-signed URLs for upload/download/stat (#5099)
- Deprecate OIDC (#5063)
- Deprecate merge API result summary - mark for deprecation (#5119)
- Support multiple storage accounts in Azure (#5096)

:ladybug: Bug Fixes
- Fix Google Storage import with relative key (#5114)
- Fix Azure import with relative key (#5123)

## v0.90.1

What's new:
- UI: allow filtering tags and branches by prefix (#5060)
- UI: Display email when adding users to groups (#4993)

Bug fix:
- Fix lua client routing (#5078)
- Fix UI new repo getting started page (#5075)
- Fix Azure ingest with pagination (#5066)

## v0.90.0

What's new:
- Authorization using Access Control Lists
- Support ADLS Gen2 (#5027)
- ui: added a "go to prefix" button (#5036)
- Add support for presigned URLs in the lakeFS API (#4985)
- change to dynamodb on-demand capacity (#4953)
- upgraded to bootstrap-5, react-bootstrap-2.7 (#5034)
- Optional Blockstore S3 region (#4956)

Bug fix:
- Reduce the use of specific catalog errors to fix missing checks (#4995)
- Fix graveler to work with empty key (#4986)

## v0.89.1

Bug fixes:

- Fix missing path for if-none-match in putObject #5156

## v0.89.0

What's new:

- UI: added a blame button to path context menu (#4888)
- Configurable repository and commit cache (#4910)
- Support MinIO (#4878)
- [Breaking Change] API delete will not return object not found (#4886)

Bug fixes:

- Refactor lakefs_export script output (#4904)
- Report all errors as messages not as Exceptions (#4914)
- API GetCommit return empty array instead of nil for no parent (#4922)
- KV scan batch size control (#4875)
- Return secondary key for secondary iterator (#4945)
- Add missing Auditing fields into logger context (#4864)
- Fix loading of duckdb every query (#4903)
- Fix CredentialFromProto can fail to decode secret (#4862)
- Fix ingest range content type is wrong (#4907)

## v0.88.0

What's new:
- Integrate duckdb-wasm to query parquet/csv files on the UI (experimental) (#4821)

Bug fixes:
- API upload object without using tmp file (#4848)
- lakectl show commit (#4834)
- Perform fewer API calls for exists (#4797)
- Fix commit to keep base entry in case staged entry with matched identity (#4825)
- Fix lakectl branch show to print commit ID (#4837)
- Fix auth's wildcard Match function (#4828)
- Check if DDB Table exist before Create to reduce excessive permissions requirements   (#4809)
- Store relative path in stage and link physical address (#4751)

## v0.87.1

Bug fixes:
- Fix UI "Authorization/My Credentials" view returns error (#4816)

## v0.87.0

What's new:

- Allow adding hooks without depending on an external service (#4779)
- Add support for S3 buckets using SSE with KMS Secret (#4726)
- Allow user to subscribe for email feature updates and/or security alerts during setup (#4667)

Bug fixes:

- [GC]: Correctly handle deleting absolute paths under the storage namespace (#4763)
- Ref lookup order now prefers commit and branch refs for user actions (#4743)
- Fix Python LakeFSClient missing API tags (#4606)

## v0.86.0

What's new:

- Add byte range support to OpenAPI (server, spec and generated clients) (#4623)

Bug fixes:
- Fix unmatch-delete log line (#4643)
- Change API event_type from enum to string (#4613)
- Fix diff refs api to return prefix_changed (#4639)

## v0.85.0

What's new:

- lakectl import - hold the same import functionality 'Import' from the UI (#4558)
- Setting KV drivers defaults the part of the configuration defaults (#4553)
- Login API returns the token's expiry with the token (#4597)
- Build Web UI using Vite 3.2.3 (#4576)

## v0.84.0

What's new:

- Add kv metrics (#4442)
- Add prefix change indicator - (API breaking change) (#4403)

Bug fixes:

- Fix UI report version as dev (#4465)
- Fix username not enriched in new credentials modal (#4454)
- Fix: remove unused diff_type from diffRefs operation (#4462)
- Fix dirty branch error for operations on uncommitted data instead of conflict (#4463)
- Fix status code return on revert merge without parent (#4498)
- Cache commit on Graveler ref manager (#4497)
- Upload data new structure for uploaded data physical addresses (#4530)
- Feature/configure additional OIDC scopes (#4533)
- Python lakefs-client library does not have a license (#4452)
- Fix large file and png preview issues (#4569)

## v0.83.4

What's new:

- **Deprecate** `lakefs import` (#4323)
- Added download capability to `lakectl` (#4418)

Bug fixes:

- Fix README.md file viewer not displaying content according to file extension (#4380)
- Fix entity creation fails when % is used in entity name (#4414)
- Fix lakeFS returns code 500 when getting absent object (#4427)
- Fix blank page when viewing objects in subdirectories (#4449)

## v0.83.3

What's new:
- lakectl autocomplete with repository name (#4320)
- Flush statistics based on events size (#4347)
- lakectl check bad response (#4331)

Bug fixes:
- Update and create policy use the user's provided ID (#4359)
- Fix KV local path using tilde (~) doesn't expand (#4330)
- Fix username not shown for OIDC users (#4324)
- Fix create repository API should return status created (#4336)
- Correctly prefix "after" params in lakeFS auth service (#4353)
- Fix create policy API and descriptor to return Conflict status (#4350)
- Fix: Auth update policy (#4355)

## v0.80.2 - 2022-10-09

This version fixes kv migration bug in 0.80.1. If you already migrated to kv successfully,
you can skip this version.

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version.
Please refer to this [upgrade documentation](https://docs.lakefs.io/reference/upgrade.html##lakefs-0800-or-greater-kv-migration) for more information on the specific migration to KV

Bug fix:
- Fix multiparts KV migration for null content-type (#4343)

## v0.83.2

What's new:
- Adding lakeSF description to python's pypi package (#4260)
- Report extended information with metrics (off by default) (#4196)

Bug fixes:
- Fix S3 gateway delete object limits check (#4240)
- Fix S3 gateway report status code and operation ID in case of an error (#4293)
- Fix UI render error inside repo error (#4301)
- Fix UI repository settings label width (#4300)
- Fix logging.IsTracing should check default logger level (#4252)

## v0.82.0

What's new:

- UI: New file viewer (#4226)
- Performance improvements: enable delete-objects as part of Graveler (#4205)

Bug fixes:

- Migrate work as part of setup for auth-api installations (#4208)
- Invite user is enabled when SMTP server is not configured (#4224)

## v0.81.1

Bug fixes:
- Fix: local-settings flag (#4200)

## v0.81.0

What's new:
- Run standalone lakeFS (no PostgreSQL!) with Badger KV for experimentation purposes. 
- Add delete operation for GC rules (#4143)
- support AWS named profile for dynamodb (#4163)
- Add pgxpool metrics to kv/postgres implementation (#4137)

Bug fixes:
- Fix: Pyramid delete before open (#4062)

## v0.80.1 - 2022-09-01

Bug fix:
- Fix PartitionIterator panic on Close (#4108)
- UI: Remove error message on conflicting diff of binary objects, show src, dst file sizes instead of diff (#4105)

What's new:
- Dockerfile update CA certificates (#4101)


## v0.80.0 - 2022-08-31

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version. 
Please refer to this [upgrade documentation](https://docs.lakefs.io/reference/upgrade.html##lakefs-0800-or-greater-kv-migration) for more information on the specific migration to KV

This is the first lakeFS version over Key-Value Store
lakeFS is decoupling from PostgreSQL and moving to a KV Store interface. 
This will provide greater flexibility and allow production groups working with lakeFS to select their backing DB of choice.  
Check our updated [Deploy lakeFS](https://docs.lakefs.io/deploy/#deploy-lakefs) page, for deployment instructions.
Also make sure to check our [Sizing Guide](https://docs.lakefs.io/understand/sizing-guide.html#lakefs-kv-store) for best practices, requirements and benchmarks

## v0.70.6 - 2022-08-30
- UI: fix focus on branch lookup while creating tag (#4005)

## v0.70.5 - 2022-08-23

Bug fix:
- Fix panic in commit under KV, with nil tombstone (#3976)

## v0.70.4 - 2022-08-23

What's new:
- Improve commit log performance for single match by adding limit flag (#3970)
- Change Histogram buckets to better fit lakeFS commands (#3902)

## v0.70.3 - 2022-08-22

What's new:
- Improve commit log performance (#3936)

## v0.70.2 - 2022-08-17

What's new:
- Improve 'commit log by objects' performance (#3920)

## v0.70.1 - 2022-08-11

Bug fix:
- Fix DB serialization error during multiple writes to the same key (#3862)

## v0.70.0 - 2022-08-03

What's new:
- Allow OIDC as default login (#3617)
- Launch a repository with Spark capabilities (#3792)
- [GC] Respect Hadoop AWS access key configuration in S3Client (#3762)
- Make GC read the expired addresses csv from Azure blob (#3654)
- Display README file if available (#3761)

Bug fixes:
- Fixed diff-viewer version by using a known fork (#3680)
- Fix cache in auth service api (#3354) 

## v0.69.1 - 2022-07-14

Bug fixes: 
- Fix crash on group listing by remote API (#3655)

## v0.69.0 - 2022-07-11

Note: this version contains performance and security improvements to the authentication mechanism.
After upgrading to this version. all current browser sessions will become invalid and users will have to login again.

What's new:
- OIDC support: manage lakeFS users externally (#3452)
- Choose merge strategy in the UI (#3581)
- Templating capability in the lakeFS web server (#3600)
- Visibility: show branch creation errors in the UI (#3604)

Bug fixes:
- When a revert results in a conflict, return code 409 instead of 500 (#3538)

## v0.68.0 - 2022-06-21

This release fixes a bug in the garbage collector.
If you are using cloud storage that is not S3 and have configured a garbage collection policy for retention, you will need to reconfigure it.  (Note that the garbage collector itself does not yet run on such storage!)
There are no changes if you are using S3.

What's new:
- UI: Show content diff for conflicts (#3522)
- lakeFS configuration for audit log level (#3512)

Bug fixes:
- Fix: Garbage Collector - Eliminate double slash in URL (#3525)
- Fix: Crash fix on `lakectl superuser` command - missing logging initialization (#3519)

## v0.67.0 - 2022-06-16

What's new:
- Garbage collection report at end of run (#3127)

Bug fixes:
- Fix: gateway remove delimiter limitation for list objects v2 API (#3459)
- Fix: UI policy view fail to update (#3469)

## v0.66.0 - 2022-05-26

What's new:
- UI: enable server side configuration to apply code snippets (#3398)
- Upgrade Spark client dependencies to be compatible with M1 (#3420)
- LAKECTL_INTERACTIVE environment can control lakeCTL on/off terminal output (#3358)

Bug fixes:
- Fix: Use repository root commit for import branch in import from UI (#3415)
- Fix: Resource leak on list hooks run (#3424)
- Fix: Create branch name validation message (#3374)
- Fix: Evict user from authorization cache to reset password (#3407)


## v0.65.0 - 2022-05-16

What's new:
- Import your data into lakeFS using the UI! (#3233, #3352)
- Airflow hook wait for DAG completion (#3321)
- Friendlier post-setup page (#3318)
- Show skipped hooks for failed action run (#3359)

Bug fixes:
- Fixed: Revert path shows up when comparing refs (#3291)
- Fixed: Glue catalog_id parsed as int (#3252)
- Fixed: login redirects to credentials page (#3319)
- Fixed: UI create repository not focused on name field (#3342)
- Fixed link to authentication page vs authorization page (#3337)
- Fixed: Some URI parameters were not encoded (#3290)

## v0.64.0 - 2022-04-29

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version:

```sh
$ lakefs migrate up
```


- Fix bug in merge - merge with no changes resolves by creating empty commit (with no data) on destinations head  (#3270)
- Fix broken content-based diff for changed objects in compare view (#3275)
- Bump metadata client version to 0.1.7-RC.0 (#3277)
- Fix logged out user redirect to login (#3283)

## v0.63.0 - 2022-04-17

- Authenticate using an external service (#3178)
- Fixed bug in repository name validation (#3155)
- Fixed bug in some argument validations (#3185)

## v0.62.0 - 2022-04-03

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will be required to run migrate, with the new version:

```sh
$ lakefs migrate up
```

Features:
- Update `commit ref` to `commit URI` for `lakectl tag create` command (#3017)
- `lakectl annotate` now defaults to a non-recursive listing (#3001)
- `lakectl doctor` command improvements. Part of #3002 (#3023)
- Don't show GetStarted for empty commit (#3041)
- Allow ingesting from a non-default S3 endpoint (#3084)
- Validate new repo isn't using existing storage namespace (#3104)
- Add additional hook locations (#3130)

Bug fixes:
- `lakectl annotate` output has superfluous spaces and blank lines (#3007)
- Fixing restore refs performance issues for old commit dups without "generation" field (#3011)
- `lakectl config` now hides secret access key (#3039)
- Fix error capturing and formatting in DB operation failures (#3025)
- `lakectl ingest` stages more objects than source s3 bucket (#3081)
- `lakectl ingest` adds multiple excess slash to object name (#3108)


## v0.61.0 - 2022-03-07
Features:
- Add merge strategy (#2922)
- DBT: add branch creation capability (#2988)

Bug fixes:
- Fixing performance issue with ref-restore of commits (#2992)

## v0.60.1 - 2022-03-01
Features: 
- Log with multiple outputs (#2975)

Bug fixes: 
- Bugfix/2935 lakectl bug on not found html (#2966)

## v0.60.0 - 2022-02-27
Features: 
- Add a "Default storage namespace" configuration (#2952)
- lakectl: add a `lakectl doctor` command to run a basic diagnose on lakeFS configuration (#2948)

Bug fixes: 
- Fix diff performance issues (#2968)
- Improve memory footprint during openapi object upload (#2963)
- Make "Everything Bagel" Jupyter notebook container support S3A  (#2946)


## v0.59.0 - 2022-02-15
- lakectl: Convert windows paths to S3 style paths on upload (#2932)
- lakectl: Allow empty commit message with a specified flag (#2927)
- lakefs: Live configuration reload will change logging level (#2949)

## v0.58.1 - 2022-02-09
- Merge operation optimized by another 20%! (#2884)
- Improved the output verbosity of the `lakectl dbt` tool. (#2895)
- Usage examples added in `lakectl repo create` command. (#2900)
- Fixed misleading errors on branch creation. (#2859)

## v0.58.0 - 2022-01-26

- Include Jupyter notebook in our everything bagel (#2832)
- Added branch existence check to createMultiPart (#2835)
- Align hadoop-aws versions in Hadoop filesystem and shade lakeFS API client (#2843)
- Remove trailing newline, make `lakectl fs cat` output identical to file (#2845)
- Prevent Graveler from deleting default branch (#2851)
- Added lakectl CLI command 'annotate' (blame)  (#2825)
- Stream output from `lakectl fs cat` rather than copying file to memory (#2852)
- Switch docker-compose "everything bagel" to use jupyter/pyspark-notebook (#2869)
- Fix docker-compose "everything bagel" for Windows users (#2871)
- Disable hooks run configuration (#2881)
- Add date flag to commit api (#2878)

## v0.57.2 - 2021-12-26
- Performance: Optimize merge by skipping ranges when source or destination are identical to base (#2822)

## v0.57.1 - 2021-12-20
- Major performance improvment in merges (#2808)

## v0.57.0 - 2021-12-21

- OpenAPI: Delete multiple objects in a single request (#2788)
- Performance: Optimize merge by skipping ranges with same bounds (#2737)
- Security check by lakeFS version and suggest upgrade (#2776)
- Include dbt in Everything Bagel (#2769)

## v0.56.0 - 2021-12-05

- Fix bug: faulty LDAP username validation (#2774)
- Fix bug: lakectl metastore create-symlink command (#2747)
- Fix bug: can't view tag when name is invalid as branch ID (#2723)
- Fix bug: can't view content diff when comparing non-branch references (#2751)
- Fix bug: Refresh page after merging from the UI (#2743)
- UI: Calculate change summary under every prefix (#2744)
- Improved readability of error messages (#2738)

## v0.55.0 - 2021-11-17

- lakefs-dbt integration: support lakeFS branches in dbt (#2680)
- UI: explore tags in objects, compare and other views (#2670)
- UI: View content and size diff between two objects (#2685)
- Logging: Add request ID fields to all DB and auth logs (#2683)
- Remove the lakefs diagnose command (#2693)
- Add an s3 block adapter configuration parameter to disable bucket's region discovery (#2705) 
- Make lakectl provide meaningful information and exit code in case of a merge conflict (#2700,#2706; Fixes:#2699)

## v0.54.0 - 2021-11-08

- Fix branch creation concurrency bug (#2663)
- Fix login button required two logins to pass (#2524)
- Multipart upload content verification failed on s3 encrypted bucket (#2656)
- Present commit history for a specific file or prefix (#2251)
- Support S3 API copy-object across buckets (#2162)
- Add copy-schema to lakectl metastore commands (#2640)
- UI: New Tags tab (#2655)
- UI: Add docs link to Setup, Create Repo, Branches, and Admin Pages (#2316)
- UI: Unifying 3 views into one in uncommitted/compare/commit components (#2602)

## v0.53.1 - 2021-10-21

- Fix ldap auth re-open control connection after it closes, and add timeouts (#2613)
- Better format server error messages from lakectl (#2609)
- Fix lakectl crash while reporting some server errors (#2608)
- Fix Improper Access Control in S3 copy-object, and API restore-refs,dump-refs, get-range, get-metarange ([GHSA-m836-gxwq-j2pm](https://github.com/treeverse/lakeFS/security/advisories/GHSA-m836-gxwq-j2pm))

## v0.53.0 - 2021-10-25

- Add support for LDAP authentication (#2058).
- Support object content-type and user metadata (#2296).
- Support multiple commits in lakectl revert (#2345).
- `lakectl diff`: support two way diff.
- `lakectl diff`: allow including uncommitted changes in the diff. 
- Fix Trino AVRO format access via S3 gateway (#2429).
- Support lakectl fs rm --recursive (#2446).
- Fix UI list users pagination (#2581).
- Add tree-view for uncommitted, compare and commit views (#2174)

## v0.52.2 - 2021-10-10
- Fix nil panic for missing configuration of Airflow hook (#2533)
- Allow more characters and different length of key/secret for user authorizations (#2501)
- Fix nil panic for missing configuration of Airflow hook (#2533)
- Fix failed to merge branch running on Windows locally - access is denied (#2531)
- Fix UI failed to load on Windows - invalid mime type (#2537)
- Fix UI path reset on branch change bug in object view (#2441)
- Fix UI changing the base-branch changes the compared-branch bug (#2440)

## v0.52.0 - 2021-10-04

- Protected Branches (#2181): define rules to prevent direct changes on some of your branches. 
  Only merges are allowed into protected branches.
  Combine these with pre-merge hooks to validate your data before it is on your production branches.
- Fix filter dialog unsearchable bug (#2460)
- Fix s3 multipart upload location url (#1779)

## v0.51.0 - 2021-09-19

- Add new "AttachStorageNamespace" IAM action.  Controls users' ability to
  create repositories with particular storage namespaces (bucket names).
  (#2220)
- Fix path encoding when checking sigV2 signatures in the S3 gateway.
- [S3 gateway] Return HTTP 409 (Conflict) when creating existing repo (#2451)

## v0.50.0 - 2021-09-05

- Fix double slash bug in storage namespace (#2397)

## v0.49.0 - 2021-09-02

- Add search locations to load lakeFS configuration. More information on
  https://docs.lakefs.io/reference/configuration (#2355)
- Fix ARNs parsing confusion when the account-ID field contained a slash or
  the resource-ID a colon.  Configurations (incorrectly) using a slash "`/`"
  to separate account from resource in the ARN will need to switch to use a
  colon "`:`".  However such configurations are probably incorrect, as
  lakeFS does not currently set account fields.  And configurations using
  resources containing a colon will now work correctly.

## v0.48.0 - 2021-08-22

- Support multiple AWS regions for underlying buckets (#2245, #2325, #2326)
- Make S3 Gateway DNS settings optional
- Fix lakectl upload fails on big uploads (#2280)
- Fix blank screen bug in UI (#1908)
- Actions secrets support with env vars (#2333)
- Reduce the number of database connections used on startup
- Validate required configuration keys blockstore.type, auth.encrypt.secret_key. This breaks existing configurations that assume a default blockstore.type of "local". But no such configuration may be for production.
- Fix incorrect time logged on DB get operations (#2341) thanks @holajiawei
- API with an unknown path should return an error (#2190) thanks @DataDavD
- Retry DB connection on Migration (#2017)
- 
## v0.47.0 - 2021-07-28

 - Hooks: support triggering Airflow DAGs (#2266)
 - Metastore tool: fix --continue-on-error flag on copy and import (#2267)
 - API: Return gone response (410) in case physical data was removed (#2264)

## v0.46.0 - 2021-07-19

 - Support post-merge and post-commit events
 - UI - add garbage collection policy configuration
 - Metastore tool - add flag to support DBFS location
 - Metastore tool - handle Spark placeholder on copy
 - Deployment of lakeFS (docker-compose) above MinIO including the following services: Hive metastore, Hive server, Trino and Spark
 - Fix LakeFS startup with Azure storage type

## v0.45.0 - 2021-07-07

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

Fix Authentication migration for garbage collection.


## v0.44.0 - 2021-07-01


Garbage Collection! Set retention rules for deleted objects. See issue: #1932


## v0.43.0 - 2021-06-24

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

- Server-side support for the upcoming garbage collection functionality (#2069)
- Add lakeFS version to the UI (#2088)
- Update lakectl branch create output to include branch name (#2130) 
- Warn about using "local" adapter for use in production (#2159)
- Explicit handling of mismatch in block store config and repository namespace according to a user setting (#2126)
- Initial database connect retry (#2131)
- Web UI Vite build tool upgrade to v2.3.7 (#2108)

### Bugfixes
- Fix lakectl read environment variables in absence of lakectl.yml config file, this bugfix unlocks the Kubeflow-lakeFS integration (#2143)
- Add UI error reporting on action failure during a commit (#2120)
- Reset the state of the delete repository dialog when it is dismissed (#2117)


## v0.42.0 - 2021-06-08


### Features
 - Support common prefix listing of diffs (uncommitted, commit view, compare view) (#2051)
 - Export using spark-submit (#2036)

### Bugfixes
 - Make authorization errors readable in the UI (#2056)
 - Verify paths used in the local block adapter are under its base dir (#2003)
 - Fix dump refs JSON output (#2040)
 - Fix adapter qualified key format to not use `path.Join` (#1994)

### Breaking Changes

 - Object listing API: previously, not specifying a delimiter would default to using "/". The default is now "" (empty string)
 - The `/config` API endpoint that returns storage configuration (used mainly in lakeFS UI), is now under /config/storage
 - Accessing the `/config/storage` endpoint now requires the `fs:ReadConfig` permission instead of `auth:ReadConfig`


## v0.41.1 - 2021-05-30

36eb6ae3 Bugfix/parquet inventory failure (#1979)
4c9b4046 Filesystem: non-atomic files rename  (#1972)
37a52ceb Fix unexpected merge conflicts bug (#1958)
13de2728 Short timeout on fetch AWS account ID (#1987)
2a553227 Support Spark SQL tables in metastore copy (#1997)
4589933f gateway on get without version should return after error (#1990)
7307f5a0 hadoopfs listFiles (#1922)
72265888 lakectl control logging (#1954)
931be4a5 lakectl log copy and update metastore operation information (#1986)
4bfead33 use a worker pool to ingest entries faster (#1964)

## v0.41.0 - 2021-05-12

2e2e005a ingest cmd [Importing data from an object store without actually copying it](https://docs.lakefs.io/reference/import.html#importing-data-from-an-object-store-without-actually-copying-it) (#1864)
851f02a4 Don't upload chunks smaller than 8192 bytes while streaming (#1885)
1a81228f Fix multipart upload failure in Azure (#1896)
9ebab7a7 Fix broken Clipboard copy button (#1904)
984cc68e lakeFS filesystem add "create" method (#1907)
5996edf8 lakeFS filesystem add "open" method (#1895)
7c22cd5c Add Java OpenAPI client up-to-date validation (#1870)
4ea65718 Add conflict response and use it when creating an existing resource (#1900)
0fad6a92 Use React with Vite (#1874)
ad188b60 Implement lakefsFS delete method (#1920)
ca962710 Mark operations unsupported by LakeFS filesystem (#1881)
f592298d Publish Java API client to Sonatype repository (#1869)
be53f583 Remove unsupported reset-to-commit option (#1946)


## v0.40.3 - 2021-05-04

* New Java and Scala API client (#1837)
* Metastore operations between  Glue and Hive (#1838)
* Fix bug where unchanged files appear as changed (#1841)
* Upgrade NodeJS (#1801)
* Added put-if-absent operation (#1823)


## v0.40.2 - 2021-04-26

 - New repository default branch from `master` to `main` (#1800)
 - Fix auth middleware - check only required security providers (#1795)
 - Entry's identity to not contain physical address path (#1805)
 - Fix logger broken caller and function detection (#1807)


## v0.40.1 - 2021-04-20

4206fcd1 UI: missing setup page (#1793)
0505c797 UI: use first parent for diff of merge commit (#1787)


## v0.40.0 - 2021-04-20

This is a big release for lakeFS with many notable improvements.

Some of these are breaking changes. It's always a tough decision to introduce a change that isn't backwards compatible,
but we felt that at this stage they represent a significant enough benefit to be worth it.

Going forward, our goal is to make as few of those as possible, as we near a 1.0.0 release.

Here are the most notable changes:

### lakeFS is now OpenAPI 3.0 compliant ✨

The lakeFS API has been migrated from OpenAPI 2.0 to OpenAPI 3.0.

[OpenAPI 3.0](https://swagger.io/specification/) includes many improvements over the previous version: Cookie based authentication, reusable query parameters, better JSON Schema support and more.

While homegrown clients that simply use the lakeFS API as a REST inteface will continue to work,
client that relied on OpenAPI 2.0 specific behaviors will stop working.

This includes the previously recommended [bravado](https://github.com/Yelp/bravado) based client for Python. For that reason, we're also releasing an officially supported Python client:

### lakeFS now ships with a native Python client ✨

It's now as simple as:

```sh
$ pip install lakefs-client~=0.40.0
```

And then:

```python
import lakefs_client
from lakefs_client.client import LakeFSClient

lakefs = LakeFSClient(lakefs_client.Configuration(
    username='AKIAIOSFODNN7EXAMPLE', 
    password='...', 
    host='http://lakefs.example.com'))
    
lakefs.branches.list_branches(repository='my-repo')  # Or any other API action
```

This client is officially supported and distributed by the lakeFS team, and will be released in conjunction with lakeFS releases, so it should always align in capabilities with the latest lakeFS versions.

For more information, see the [Python Client Documentation](https://docs.lakefs.io/using/python.html).

### Native Spark client, allowing to export a commit (or set of commits) to another object store ✨

Using Apache Spark, lakeFS users can now quickly export the contents of a branch to an external location (say, S3 bucket). Exporting committed data will be parallelized using Spark workers to support copying millions of objects in minutes.

This is the first feature released based on lakeFS' Spark integration (soon to be followed by data retention for stale objects), and a native `lakefs://` filesystem support for Spark).

For more information, see the [Export Job configuration Documentation](https://docs.lakefs.io/reference/export.html).

### lakeFS standardized URIs ✨

The lakeFS CLI now supports a standardized URI in the form: `lakefs://<repository>/<ref>/<path>`.
Additionally, the CLI now allows setting a `$LAKECTL_BASE_URI` environment variable that, if set, will prefix any relative URI used.

For example, instead of:

```sh
$ lakectl diff lakefs://my-repository/my-branch lakefs://my-repository/main
$ lakectl fs ls lakefs://my-repository/my-branch/path/
```

It's now possible to simply do:

```sh
$ export LAKECTL_BASE_URI="lakefs://my-repository/"
$ lakectl diff my-branch main
$ lakectl fs ls mybranch/path/
```

For more information, see the [CLI Command Reference Documentation](https://docs.lakefs.io/reference/commands.html).


### Complete UI Overhaul

Making it faster, more responsive and contains many improvements to pagination, commit browsing and action views.

<img src="https://gist.githubusercontent.com/ozkatz/81cee863dee268769cd3aa5ea5fddad5/raw/c7aa7c519a27408dcd99e705be286e778ea7876d/frame_generic_light.png" alt="UI Screenshot" width="600" style="text-align: center;"/>

### Full Feature list

- `[UI]` Complete UI overhaul 💅 (#1766)
- `[Spark]` Spark client that allows exporting from lakeFS to an object store ✨ (#1658)
- `[Metastore]` Support metastore copy between two different hive metastores ✨ (#1704)
- `[API Gateway]` **BREAKING**: Migrated to OpenAPI 3.0 💣 (#1667)
- `[Python SDK]` Native lakeFS Python Client ✨ (#1725)
- `[Graveler]` **BREAKING**: commit parents order for merge-commits are now [destination, source] instead of [source, destination] 💣 (#1754)
- `[CLI]` **BREAKING**: `lakefs://` URIs are now standard, replacing `@` with `/` to denote ref 💣 (#1717)
- `[CLI]` `$LAKECTL_BASE_URI` prefixes all lakectl URIs for more a human-friendly CLI 🥰 (#1717)
- `[CLI]` Support non-seekable stdin (`-` arg) in "fs upload" command 🥰 (#1672)


### Bug Fixes

- `[S3 Gateway]` Avoid logging v2 sigs on failure 🔒 (#1679)
- `[Graveler]` Limit length of Graveler serialization 🐞 (#1682)
- `[Graveler]` Fix merge large changes performance  (#1652)
- `[S3 Gateway]` Handle no path for delete objects in gateway 🐞 (#1708)
- `[API Gateway]` API merge message is optional 🐞 (#1710)
- `[API Gateway]` Fix auth pagination 🐞 (#1755)
- `[API Gateway]` List repository actions should not check branch existence 🐞 (#1743)


As always, we hang around at [#help on the lakeFS Slack](https://docs.lakefs.io/slack) to assist and answer questions!


## v0.33.1 - 2021-03-18

b253f817 Actions tab UI (hook runs) (#1564)
cd8dbce9 Adding arm64 to our release binaries (#1585)
7e227b32 Convert merge errors to bad requests (#1555)
9b3ac8e2 Special warning before delete import branch (#1582)
5b31659b Upgrade to Go 1.16.2 (#1619)
c68123ef batch hot paths for a very short duration (#1618)
e053cd36 gateway requests reaching ui handler to fail gracefully (#1578)
759baa85 lakectl validate action file (#1601)


## v0.33.0 - 2021-03-01

### Main benefits

  * Azure Blob Storage support Azure AD authentication 
  * Webhooks: actions mechanism for running hooks on pre-commit and pre-merge (CI/CD v1 milestone)


  * Protoc and swagger validation as part of CI using docker (#1498)
  * Expose API metadata handlers (#1494)
  * Fix diff throwing 500 instead of 404 when ref/repo not found (#1492)
  * Make swagger.yml pass schema validation (#1495)
  * Fixed filtering repositories in UI in case an exact repo name was passed (#1493)
  * Enable delete repository action from the UI (#1372) thanks @shamikakumar


## v0.32.1 - 2021-02-17

0bf71b72 don't sign EXPECT header (#1477)


## v0.32.0 - 2021-02-17

484738c8 Add Azure adapter (#1444)
f2ed0869 Add "get metarange", "get range" API endpoints (#1465)
611d9c02 basic client config to pass default max conns per host. (#1455)


## v0.31.2 - 2021-02-11

a9811832 Different dbpool for branch-locker (#1447)


## v0.31.1 - 2021-02-09

c3f89e48 Import should use repo default branch (#1422)
a284fc31 Pass s3 retry param to the aws config (#1424)
c16d1f78 Switch left and right branch ref in UI diff pagination (#1426)
80e423a0 generate CLI docs automatically from command help text (#1408)


## v0.31.0 - 2021-02-08

e307d37e Control number of concurrent range writer uploaders (#1351)
eb4097c0 Remove SSTable reader cache, use shared pebble in-memory cache (#1332)
33573d7b allow revert merge commit (#1353)


## v0.30.0 - 2021-01-26

This is the first release of [lakeFS on the Rocks](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing)! :tada:
Included is a big change to lakeFS' data model, which is now much closer to Git's.


### Main benefits

1. Metadata representing committed versions now lives on the object store itself (instead of on PostgreSQL)
2. As a result, PostgreSQL will now typically be smaller and do much less than in previous versions
3. Performance should be more predictable, and noticeably better across the board, especially on large diffs/commits/merges

### Upgrading from previous versions

https://docs.lakefs.io/deploying/upgrade.html


## v0.23.1 - 2021-01-21


774e935c Fix delete objects permissions bug (#1260)

## v0.23.0 - 2021-01-18

a73e4d62 Add nessie tests for delete (#1199)
a4ab2eb3 Add physical address to the stat response (#1204)
dc6a9688 Apply server-side default log amount limit always (#1182)


## v0.22.1 - 2021-01-11

575533ae Separate S3 gateway to various http handlers (#1166)
cb9c42b8 Configuration: use constants for flags (#1148)


## v0.22.0 - 2021-01-03

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

8731b60f Use gaul/s3proxy to fallback to AWS (#1113)
60c9a64c Local adapter clean user input before file access (#1037)
28a08d4a webui update packages minor and remove typescript from dev (#1094)


## v0.21.4 - 2020-12-20

0aaf99c2 Fix bug in import tool: handle empty orc file (#1089)


## v0.21.3 - 2020-12-20

7c2c3052 Import Tool - filter by key prefix (#1085)
76d88deb Multipart upload test enhancement (#1061) (thanks, @Sufiyan1997!)


## v0.21.2 - 2020-12-15

c6ff2556 Correctly measure duration of Query (#1042)
11b432d4 Fix race during concurrent cache entry creation (#1053)
2a705066 TierFS enhancements (#1008)
2e0e8356 bug fix firefox upload error unsupported media type (#1064) (thanks @mschuch)
46e16492 minor fixes for code doc (#1049)
77ca99e4 staging manager: drop by prefix (#1036)


## v0.21.1 - 2020-12-13

93b4222b Add UI delete branch #956 (#991)
ec39a820 Add initial implementation of Graveler (#1019)
11a13f8b Allow lakectl to run without a config file (#1040)
48ff2888 Cataloger create branch by ref (#1020)
153bf852 Change graveler iterator interface (#1014)
f25a4e53 Rename forest package and move it to graveler (#1035)
78dea4f9 Simplify commit iterator logic (#1017)
182bb7aa Upgrade webui ini package to fix GHSA-qqgx-2p2h-9c37 (#1051)
8aada342 Added graveler tree interface (#1043)
211afbb1 Avoid int cast without limit check (#1038)
4895faed Fix gateway branch name validation (#1024)
a8dc3440 Fix javascript lint issues (#1039)
12e854ce List commits limit back default amount (#1045)
d8f20606 Use default amount for ui list branches amount (#1021)


## v0.21.0 - 2020-12-08

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

d19107da Add test for refs diff (#1012)
4ce4df1e Fix gitleaks configuration (#950)
68cdf0c2 Generate {application,text}/xml headers on gateway by client's choice (#992)
c5a49553 Set default value for all amounts in swagger (#1011)
5ee62e05 pg refs manager for graveler


## v0.20.0 - 2020-12-06

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

e86f416 bug fix Merge may apply partial changes due to concurrency issues (#1007)
99e89ec bug fix Committing an object deletion to master erases object from all commits to master - #997 (#1000)
5e34175 Change sstable interface to match new graveler package (#1003)
3084355 Feature/staging mgr postgres (#981)
b1b3858 Graveler transform catalog interface (#993)
e88f3c7 Immutable tiered storage (#962)
cf041f8 Check and fix the use of errors.As (#1004)


## v0.19.0 - 2020-12-03

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

3aa396f Add "lakectl fs upload --recursive" flag (#979)
18bf790 Add logs for gs nessie failures (#990)
a57e068 Extract multipart upload implementation from mvcc cataloger (#989)


## v0.18.0 - 2020-12-02

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

7b08240 rollback committed changes blocked by child branches (#980)
4f89db9 Add interface for async handling of sstable writer flushing and closing (#978)
83478b7 Catalog rollback to commit branch verification (#988)
9623170 Merge direction #955 (#968) (thanks @shamikakumar)
06806c8 Rocks catalog interface (#959)
c152e2c add installation id after init (#983)
90600f9 allow empty installation id (#942)
4b28f2e catalog interface, change Dereference to RevParse (#972)
dc69cc4 cataloger rocks initial connect to catalog (#976)
fdd0ca1 check before building and pushing an existing image in benchmark flow (#974)
24fb08c sstable interface for committed data (#971)
b8bdcc0 sstable writer to accept a single rocks.EntryRecord arg (#975)


## v0.17.0 - 2020-11-26

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

8f6365e Add API key specification to "lakefs setup" and use that in Nessie
1ee9a61 Add continuous export configuration
331f771 Allow access key configuration from "lakefs superuser" command
db02f63 Avoid "magic" number in exit(2)
24974e3 Call gen_random_uuid directly from the public schema (#944)
35a8ecb Clean up time.Duration type multiplication in Tx retries
a4b3ec0 Continuous export: start a new export after each commit or merge
4c888fa Create cataloger based on configuration (#951)
25b500e Fetch lakeFS envariables in nessie auth config
b3acce7 Improve logging and clean up flows
5740599 Log db params in nessie auth_test
38c4763 Make "lakefs init" work when _not_ specifying access_key_id
99b0c86 Add ability to specify Access Key ID and Secret Key in setup endpoint (#908)
4526447 Export lakeFS branch to S3 continuously (#534)
2c137c6 local adapter - respect namespace in objects full path (#938)
95e2125 lakefs init broken: "access-key-id: flag accessed but not defined: access-key-id" (#940)
147788a Add repair to export (#943)
84a52ee Continuous export (#949)
6f8a0fe Migration validate version should not create schema_migration table on check (#953)
5cea104 Move cataloger mvcc implementation under a different package (#946)
04e6499 Return installation ID as part of health check response (#964)
5e77681 Run hooks outside of transactions
b810243 Share lakefs config values between nessie and lakefs containers
f32712f Test (empty) diff between a ref and itself
b35b36e [CR] Flatten if/elses in catalog/mvcc/cataloger_merge_test.go
3e63322 [CR] GetPrimitive doesn't call pgxscan, use pgx directly there
40fec4b [CR] Refactor merge & commit hooks
25d2bec [CR] Revert envariables change in nessie GH action + compose files
6985dba [CR] Use cobra.ExactArgs, SQL IF [NOT] EXISTS
5976728 [CR] Validate nonempty access key ID, secrets in Swagger
99770f8 [CR] pass hook values by value
5cd4d16 [GH actions] re-run lakeFS in nessie action
84a50ab [bug] verify required positional args are present
634e85a [bugfix] return ErrNotFound correctly (#952)
4871e49 [checks] use a shorter error line to pass err113 golangci check
4ebed6c [nessie] allow nessie to connect to postgres container
d55eb5d [post-rebase] Revert "status" back to "state" in field names
79ff970 add repair option to export when running export with repair it will consider the previous errors as repaired and continue from last commit
8782fd4 change ExportState to work with one function that calls a callback with the current status and updates the status regarding to the values returned from the callback
577388e change requests SQL fixes export callback return newRef add endpoint for repair
af3c650 fix calling to testing.T inside a goroutine
2e2fc4f fix duplicate migration
74ac713 helm docs: add extra env vars (#937)
0f3084a lakefs diagnostics query db version (#939)
1183d77 local adapter - respect namespace in objects full path
b69c89a make postgres connection string optional (#948)
28e4052 rename lakefs init to setup and keep init as alias (#961)


## v0.16.2 - 2020-11-16

34185da Diff performance improvement by trim lineage  (#921)
148b8e8 Revert "Share lakefs config values between nessie and lakefs containers"
e23adc4 S3 reference and example parameters to work with MinIO
46c7a57 Share lakefs config values between nessie and lakefs containers
a83dc02 add integration test for export


## v0.16.1 - 2020-11-13

78b6d8f Fix fail export in case export is currently in progress #914
c6674e6 Document export and unhide from CLI #915
825006f WebUI get config called only on create repository #917
19a4357 lakefs init and setup handler share code #919
1d79c2d Fix export to s3 not working due to wrong paths #920
c3fd501 Update ddl/000009_export_current.down.sql

## v0.16.0 - 2020-11-12

This release requires running database migration.
The lakeFS service will not run if the migration version isn't compatible with the binary.
Before running the new version you will require to run migrate, with the new version:

```sh
$ lakefs migrate up
```

* The older version of lakeFS will not work with the new migration version

7a78820 Diff working in a separate goroutine from the merge
365cc9f Fix wrong UI error when repo name contains underscore (on repo creation) 833 (#879) (thanks @Sufiyan1997)
dd4a6fb When uploading a file, the path in the repo should be visible #835 (#866) (thanks @shamikakumar)
24f18e4 Configure AWS clients to retry S3 ops when possible
0416813 "mkdir -p" destination directories for local adapter Put, Copy
8e579d0 Add export state logic and tests
328b134 Align repo id to repo name (#874)
9dddbac Change export tasks generation to be iterative
da896de Create extension pgcrypto in "public" schema
5b4d41d DB scanner options by value with an option to update AdditionalWhere
9ebd4d7 Enable GetCommit to work when branch name, returns the last commit on branch
cccb491 Explicitly use PUBLIC schema
f7c0dbc Fail a task if it retries too many times
9f593f3 Fix repo id to name in the UI (#881)
063ce17 Fixed and responded to all review comments
9dd26be Hide "export" command
725e181 Refactor database to use sql + scany instead of sqlx
c82c549 Remove SQLX usage from parade
bf99b95 Restore a name for dreaded magic number of 5 retries
40f916b SQL - replaced "values" and string concat with array and unnest
a7affc3 Use pgx instead of sql
1eeb2a4 [CR] DROP TABLE... IF EXISTS
72c25aa [CR] Remove probably-unused fields from monitoring
7201f13 [CR] Rename Get -> GetPrimitive, GetStruct -> Get
3ff1f62 [CR] Use standard db:"-" tag, copy all input to convertRawCommit
cf229a5 [CR] cleanups: "go mod tidy", deferred Close wrapped in unneeded func
ccf7ccf [bug] file tasks depend on nothing
4ec6f1d [export] Add current export status to DB
1a369aa [lint] missing error return caught!
5394c77 [lint] use proper Go-style comment for TaskDataColumnNames
1fd05a6 [parade] Move DDL under ddl/
08c9a5c [parade] count failures in direct predecessors, pass them on tasks
e4dbf3b [rebase] Fix dbPool closure in new superuser command
d35ed36 add endpoint for export
88f41e9 add export state - run start and end with a callback between without the need to open a transaction outside of the cataloger fix - missing if error db.ERRNotFound instead of ErrEntryNotFound
d440053 add filtering by relevant commits to branch scanned
332f7c4 add tests for lastKeysInPrefixRegexp
e4fb6ee after merging with changes that happened in the code, and debugging it
04c9489 change ExportConfig action remove sizes from varchar in parade
ae06cf6 change authorization for export commands change error with debug in case parade migration didn't run yet
6df779c change merge isolation level in the merge transaction from serializable to ReadCommitted
c8dd42d change operationID from executeContinuousEcport to RunExport return export ID in API
98a9132 change permissions for executing continuous export
f8cb19b change task_generator tests to work with export ID generator
1916c12 check context just in query execute
78f0f21 count records, table sizes and log errors
77120b3 default output filename for lakefs diagnostics
df06d15 dev compose file - use local dockerfile for our docker-compose
9c0cff6 extended max commits in brench, added limit to sql
de39e35 fix diff same branch run on left side lineage (#875)
fff50f2 fix div thanks to Shamika Kumar (#867) (thanks @shamikakumar)
f85b6e8 fix same branch diff should compare by a checksum (#878)
47bb5af fixed bug in sorting commits, and in deciding when to use "between" instead of
7574553 generate a status file on done
a41923f increase read buffer for branch
a49707e lakefs diagnostics command (#889)
021b307 limit number of commitIDs on branch scanner select
96e6a03 merge from parent - remove accesor branches that did not change from the parent branch
00d3eb2 migrate goto with force support (#872)
be26f36 modify installation documentation to require 1.25.4+ docker-compose version
8355201 modify sql script for commits index name
8a38e7d moving merge activity to doMerge routine
f5c43ae no need to return export status and message in case of error
e54749b preformed gofmt of diff scanner
665ea06 release checklist (#870)
26e2160 remove select * from big tables and use statistics
342ea0f remove size constraint from task id
f53b27c remove trimmedLineage from DBScannerOtions, and move them to lineage scanner
386e103 remove unused GCS parameters
892f092 require export-path flag on export set config command
cce32b6 scanner skip to next path
6cf71d5 show lakefs log on Nessie error (#876)
f8f59fd test code to call collector and verify expected files are collected
c9de871 turn diff into a scanner, and make merge apply the diff changes incrementally
923be9f upgrade react-scripts to v4.0.0 (#886)


## v0.15.0 - 2020-10-26

8527b2a Add new ReadConfigAction auth action for reading config (#837) (thanks @daniel-shuy )
2a2a8d5 Callhome to collect heartbeats (#841)
4032a01 Configure branch continuous export: Swagger defs and API handlers
1626f37 Diffs are now on references not branches (#863)
89b8b7d Feature/db read entry - read entry/s using simple union sql, instead of the complex views/sqEntriesLineage (#783)
0bba0ca Fixing terraform version bug (#860)
0f923de Implement cataloger current diff using scanners (#790)
0b2f3ba Validate dependencies licenses (#836)
e26816a add option to set a fixed installation ID
bd52e00 bug fix conflict result compare type (#829)
8d333ed change environment variable for installation id to be with underscore instead of dash
1ab4684 change handler to actor document actor
6c6351b child diff simplify check - compare delete and content first (#845)
99e1bd1 diff between two references on the same branch (#842)
b7f7cbc diff with additional fields support (#851)
4a51df8 lakefs new repositories with import branch as parent branch by default (#810)
00c7c08 lakefs superuser command for adding more admin users (#864)
7d7d611 move action_manager to parade document action_manager add data to logs
3021a8a move catalog errors to errors.go (#847)
1270083 remove extra space from error message (#820)
1f3cd72 replace current branching model with a per-use-case set of models (#824)


## v0.14.0 - 2020-10-15

a38f158 Partial index for uncommitted entries to skip full scan on select changes (#816)
7d1246e Fix lakectl fs list showed first page (#815)
05cd0b0 Repository default branch as create branch default source branch #652 (thanks @iamRishabh07)
b78e5c6 557 lakefs import improve usability (#800)
ab0332a 724 BI collection improvements (#770)
58ab9d1 Merge operation added to benchmark tests (#802)
2020328 Addition of config endpoint to api that currently retrieves only blockstore type and using it in the client (#751)
ee09a87 Allow Nessie reruns by reusing images
0030f9a Replace alert in UI with nice modal in case of delete #653 (thanks @shamikakumar)
dbba26a Improvement/import tool progress indication (#804)
10a211c Override the default Prometheus histogram buckets (#775)
ed47a62 Post-merge broken autogenerated js/swagger.yml
a6d2d6e Requirements doc for task (queue) management API
40643a7 Silence checks warning: don't try to pass difference by reference
f820167 Skip Nessie testing when secrets are unavailable (#789)
252397f Skip benchmark DB snapshot upon delete (#782)
886ca34 Use build number as CloudWatch logs group (#777)
b9c84e5 Use deleteTasks to clean up every test
589b4f0 Use pgx/stdlib to access pgx Conns directly from the DB
be24c89 Wrap all up/down DB migration scripts in a transaction (#772)
32af7f7 Fix misleading error when trying requesting for branches on non-existing repo #528 (thanks @sarathsp06)
87c82b6 Block store adapter copy support
547f5f2 lakefs-loadtest db entry create (#814)
907833a replace repo => repository in logs


## v0.13.0 - 2020-10-04

Use max commit ID value for uncommitted changes for min_commit (#742)
Set default server endpoint URL for lakectl (thanks @eylonronen)
Release notification to slack's news channel (#736)


## v0.12.0 - 2020-10-01

995d5ba lakefs import optional merge support (#726)
8d9257e Fix break in ref parse in lakectl fs ls (#728)
01cb46e Cataloger entries scan iterator (#623)


## v0.11.1 - 2020-10-01

0f4fb7f Process diff results with unlogged table (#685)
4aa55f6 Fix display bug in import tool progress bar (#678)
e438f84 Use go v1.15.2 (#665)
c456c34 Adding security check workflow (#662)
35278e4 Benchmark env (#631)
d7d8368 Fix swagger errors #658 (#659)


## v0.11.0 - 2020-09-27

81a4672 Import API using lakefs cli (#621)
9794f4c Add manual trigger option to Nessie
efdafc7 Benchmark ListEffectivePolicies (#638)
876317a Database interface align with Tx (#642)
b3a608e Fix empty dump from nessie's runs (#633)
4bace3e Fix merge after delete (#622)
4933197 Merge pull request #660 from treeverse/feature/manual-actions
46216dd Remove "Forums (coming soon)" from Community page (#646)
157cb71 Removing coming soon from community.md (#651)
604beba Try again without an explicit branch specification
af4b76d Basic welcome banner to lakeFS run (#635)
b2568aa Bugfix in import branch creation (#664)
c96a006 Change compose execution in quickstart and README (#640)
e0410fd Nessie control stats and ignore all dev versions when post stats (#626)
2b7fcf2 Remove unused and fail before access nil values (#624)
eb57df4 Updated node-forge to fix CVE-2020-7720 (#637)


## v0.10.2 - 2020-09-17

080bae0 Fix #619 - Entry not found after delete entry on the parent branch


## v0.10.1 - 2020-09-15

a48d513 Bugfix installation id not set and collector posting data is dropped (#616)
a1dae12 Bugfix lakectl no diff output - client pagination (#614)
1aff5ce "make build" so the exact copy of swagger.yml is copied (#611)
84212be Create branches, commits, merges using time on DB
aedb342 Create repository using time on DB
f387784 Merge pull request #610 from treeverse/bugfix/use-db-time
b994757 [CR] Compute close time matches
aa21c37 docker-compose use lakefs stats enabled from env if needed (#612)


## v0.10.0 - 2020-09-14

Backwards incompatible changes were made to the diff and merge API, make sure you update your client (lakectl), API and reload the UI.

9e40dce Feature/import api orc - support importing from ORC inventories (#548)
bdb71d2 Feature/diff pagination - support listing large merge/diff changes (#583)
32d83d7 Modify main unique index on entities - performance improvement 
d1a2423 561 docs snippets copy button (#590)
ad68dc8 AWS AccountID to github secret (#596)
d89843f Add Nessie's validation step of files stored in bucket (#566)
9e4f31a Add a python API usage page to docs (#584)
d333a48 Benchmark tests design (#573)
ad85efa Build docker image once during build (#580)
ece653d Chore/Nessie run system tests on Google Storage (#568)
fe5a751 Docs let jekyll-seo plugin set the documentation header title (#602)
60faab9 Fix postgres dump (#576)
8a42dd0 Improvement/docs copy button tweaks (#597)
51cc28e Merge pull request #589 from treeverse/bugfix/retention-config-link
d3e18ee Merge pull request #599 from treeverse/YaelRiv-patch-2
a92bf43 Merge pull request #600 from treeverse/docs/branches_image_smaller
c2d2ed6 Merge pull request #605 from treeverse/docs/gcs
11be374 Nessie improvements (#556)
34e6bc2 add the $ for bash code (#598)
229b868 change display-name to username (#578)
607e9e2 create entries batch multiple entries into single insert (#550)
85e87e4 fix bug: wrong timestamp after ORC import + testing (#607)
b0b0ce3 license badge (#558)
cdda1ac make image smaller (#555)
47c41f9 reference for google storage
4bf9f0b remove docker expose port (#570)
f206260 remove old scripts from repo (#553)
4704e7f rename index changes migrate (#591)
95bb4bf sanity test using Nessie (#559)


## v0.9.0 - 2020-09-01

bf51115 Add Nessie's merge and list test
e7219cf Add generic pagination in auth and use it to write more better tests
7f0a1d0 Add multipart test to nessie (#541)
ea611bc Allow reuse of db container when testing locally (#464)
2577b50 Break quickstart to several pages
a9bce37 CR: No underscores in package names when renaming imports
8f91f92 CR: add context to default logger, avoid logging nil error
65739fe Combine Nessie's endpoint and scheme to 1 flag
49bd94a Docs to show setup options for portions of the bucket
e14eeab Docs/update helm values example (#523)
615d940 Document global required configuration
3b5b144 Ensure commit order based on commit sequence order
c067d5f Feature/list commits with children (#458)
1d967af Feature/read batch (#497)
548ac59 Imporvment/quickstart docs revisit (#465)
827373b Improve logs: expiring nothing is OK, add fields
3d4c458 Initial test for ListUserCredentials
b537d92 Kubernetes in quickstart ! (#520)
fc38a97 Make expiry safe from racing against entry dedupe
d3b22f5 Move setup handler to swagger
c4274ac Nessie to run on master merge (#519)
05a497f Pass context and logger to db non-Transact methods
85ce0de Playground Design (#507)
8e03b7a Redirect quickstart.html
defa439 Refactor config: extract block, auth factories
9c2e4bd Remove gender from familial nouns (#475)
16bbeac Return (just) params from config to build stats buffered collector
4081f10 Revert "Remove retention documentation"
1eae3b2 Run Go CI tests also on merge-to-master
de3b9f7 S3 custom endpoint (#499)
85ee626 Show "lakefs expire" command
a01ed6a Update README.md (#474)
cd950c1 Update README.md (#522)
6d08ed2 Update recommendations.md (#467)
ef5b755 Update webui package to fix vulnerabilities (#479)
5845187 Use only branch master for badges (#480)
93b5537 Use refactored config->params->factory flow to construct objects
c10905d [CR] Add catalog_object_dedup.deleting column using go-migrate
cd3b60b [CR] Log durations via a wrapper function and casts
28b64d7 [CR] Place comment on column rather than in source
599772a [CR] Take logger *only* from context
e133d30 [CR] test pages have expected size and rename confusing var
1283374 append chunk until MaxPartsInCompose size (included)
04ccd97 auto migrate db on connect (#544)
08c66b5 basic gcs storage adapter no expiry or multipart upload (#485)
aea1c1b bugfix - disable URI escaping in base signer
227e2d5 configurable db connection parameters (#533)
e073d93 do not use prefix of configuration key in viper.IsSet (#420)
818d6c1 fix bare domain name on sig v2 (#471)
ba9ae6e fix get content by range - calc the right length (#514)
518dab5 fix misspelling (#461)
c7a0497 fix multipart using only uploadID
4223a56 fix pprof path bug (#546)
e2e96c7 fix storage type namespace resolve for gcs (#506)
3b89cae fixed no-color cli flag description (#511)
24a7ba8 gateway playback gcs support
652f6df get gateway/testdata folder back and create recordings when needed (#545)
8ede96d golangci-lint action and lint fixes #428 (#421)
2256aea gs adapter (#509)
759f708 gs multipart upload basic support using bucket listing
3d7604d multipart compose with 10000 limit
96fbc64 remove symlink workaround
943c99f remove unused test function (#469)
d3c6325 removed outdated warning (#488)
b21bb81 replay translate ID on all adapter methods
0eeeb72 update gems (#456)
cb94eb6 use testing short flag to skip integration tests (#455)


## v0.8.2 - 2020-08-06

0.8.2 is mostly a bug fix release, with some non-functional improvements to the project structure, linting and testing. 

7c68057 Add copyright NOTICE file (#408)
daae511 Check makefile dependencies
38d2eef Feature/add tests list entries (#407)
0275da3 Feature/sort manifest files (#375)
bc6b120 Fix linter and TODOs (#397)
2b04326 Get access key ID from AWS session
c2cd466 Publish coverage information to CodeCov (#410)
b02bdfe Remove DedupFoundCallback type (#396)
cc43b25 Support writing reports for expiry S3 batch tagging
daad87c add docker-compose to root
1e1b40a add domain name configuration to docs
c5d114b bug fix -  show deleted directories (#415)
1f76c7f close ref dropdown when clicking outside (#418)
7e1680d fix: policy validation uses wrong case for effect values (#452)
0cf054a nolint in embedded struct (#417)
15ffc4b prevent db log error when loading installationID (#406)
ad370e4 removed unionQueryParts array. - now there is only branchQueryMap (#409)

## v0.8.1 - 2020-08-03

🎉 This is the first official open source release of lakeFS
