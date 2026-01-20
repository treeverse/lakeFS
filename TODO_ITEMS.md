# Go Code TODO Items

This document contains a comprehensive list of TODO items found in the lakeFS Go codebase.

## Summary

| Category | Count |
|----------|-------|
| Technical Debt / Code Cleanup | 14 |
| Testing Improvements | 8 |
| Error Handling | 7 |
| Feature Enhancements | 11 |
| Architecture / Refactoring | 12 |
| Bug Fixes / Known Issues | 10 |
| **Total** | **62** |

---

## Technical Debt / Code Cleanup

### 1. Go 1.23 Time.After Memory Leak
- **File:** `pkg/distributed/mc_owner.go:156`
- **Owner:** ariels
- **Description:** Rewrite once on Go 1.23, which no longer leaks time.After
- **Priority:** Medium

### 2. Spin/Wait Logic Improvements
- **File:** `pkg/distributed/mc_owner.go:192`
- **Owner:** ariels
- **Description:** Spin only once (maybe use WaitFor, or chain requests)
- **Priority:** Low

### 3. Fair Request Processing
- **File:** `pkg/distributed/mc_owner.go:196`
- **Owner:** ariels
- **Description:** Be fair, at least in the same process. Chaining requests
- **Priority:** Low

### 4. Validation Logic Refactoring
- **File:** `cmd/lakefs/cmd/root.go:58`
- **Owner:** niro
- **Description:** All this validation logic should be in the config package
- **Priority:** Medium

### 5. Azure Buffer Release Issue
- **File:** `pkg/block/azure/chunkwriting.go:136`
- **Owner:** niro
- **Description:** Need to find a solution to all the buffers.Release
- **Priority:** High

### 6. Azure Implementation Refactoring
- **File:** `pkg/block/azure/chunkwriting.go:284`
- **Owner:** niro
- **Description:** Consider implementation refactoring
- **Priority:** Medium

### 7. Azure Client Cache Expiry Workaround
- **File:** `pkg/block/azure/client_cache.go:34`
- **Owner:** Guys
- **Description:** Dividing the udc cache expiry by 2 is a workaround. Can be removed once using hashicorp/golang-lru expirables
- **Priority:** Medium

### 8. Re-use Configurable Path
- **File:** `pkg/graveler/retention/garbage_collection_manager.go:196`
- **Owner:** ariels
- **Description:** Re-use configurable path
- **Priority:** Low

### 9. Unify Descending IDs Implementations
- **File:** `pkg/graveler/retention/garbage_collection_manager.go:253`
- **Owner:** Unassigned
- **Description:** Unify implementations of descending IDs
- **Priority:** Medium

### 10. Login Command Redirect Status Code
- **File:** `cmd/lakectl/cmd/login.go:141`
- **Owner:** ariels
- **Description:** Change back to http.StatusSeeOther after fixing lakeFS server
- **Priority:** Medium

### 11. Login Command Timeouts
- **File:** `cmd/lakectl/cmd/login.go:161`
- **Owner:** ariels
- **Description:** The timeouts on some lakectl configurations may be too low
- **Priority:** Medium

### 12. Rename Keys
- **File:** `pkg/api/auth_middleware.go:427`
- **Owner:** ariels
- **Description:** Rename keys
- **Priority:** Low

### 13. KV Memory Store Value Decoding
- **File:** `pkg/kv/mem/store.go:275`
- **Owner:** ariels
- **Description:** Decode values? Unfortunately DumpValue does not know the key, which is needed
- **Priority:** Low

### 14. Git Template Changes Handling
- **File:** `pkg/git/git.go:241`
- **Owner:** niro
- **Description:** How to handle better changes in templates?
- **Priority:** Low

---

## Testing Improvements

### 1. Fake Graveler Implementation
- **File:** `pkg/catalog/fake_graveler_test.go:26`
- **Owner:** Unassigned
- **Description:** TODO implement me
- **Priority:** Medium

### 2. Fake Graveler Repository ID Handling
- **File:** `pkg/catalog/fake_graveler_test.go:179`
- **Owner:** nopcoder
- **Description:** Handle repositoryID
- **Priority:** Medium

### 3. Esti Test File Naming Convention (1)
- **File:** `esti/webhook_server_util.go:4-5`
- **Owner:** niro
- **Description:** All the unused errors is because our esti tests filenames are suffixed with _test. Need to rename all esti test file names to use test prefix instead of suffix
- **Priority:** Medium

### 4. Esti Test File Naming Convention (2)
- **File:** `esti/esti_utils.go:4-5`
- **Owner:** niro
- **Description:** Same as above - test file naming issue
- **Priority:** Medium

### 5. Generalize lakectl Test
- **File:** `esti/lakectl_test.go:910`
- **Owner:** barak
- **Description:** Generalize test to work all supported object stores
- **Priority:** Medium

### 6. Check Object Length
- **File:** `esti/s3_gateway_test.go:764`
- **Owner:** ariels
- **Description:** Check actual length of object!
- **Priority:** Medium

### 7. Add Range Changed Test
- **File:** `pkg/graveler/committed/diff_test.go:279`
- **Owner:** Guys
- **Description:** Add test for range changed
- **Priority:** Medium

### 8. Add Open/Closed Tests
- **File:** `pkg/api/controller_test.go:6527`
- **Owner:** niro
- **Description:** Add tests for open / closed after we implement update
- **Priority:** Low

---

## Error Handling

### 1. ACL Filter by Email Workaround
- **File:** `contrib/auth/acl/controller.go:307`
- **Owner:** Guys
- **Description:** Workaround to skip in case of filter by email. Only used in lakeFS email authenticator which isn't supported
- **Priority:** Medium

### 2. Head Object Error Distinction
- **File:** `pkg/gateway/operations/headobject.go:30`
- **Owner:** Unassigned
- **Description:** Create distinction between missing repo & missing key
- **Priority:** High

### 3. Get Object Error Distinction
- **File:** `pkg/gateway/operations/getobject.go:78`
- **Owner:** Unassigned
- **Description:** Create distinction between missing repo & missing key
- **Priority:** High

### 4. List Objects Incorrect Error Type
- **File:** `pkg/gateway/operations/listobjects.go:265`
- **Owner:** Unassigned
- **Description:** Incorrect error type
- **Priority:** Medium

### 5. Auth Service Error Types (1)
- **File:** `pkg/auth/service_test.go:376`
- **Owner:** Guys
- **Description:** Change this once we change this to the right error
- **Priority:** Medium

### 6. Auth Service Error Types (2)
- **File:** `pkg/auth/service_test.go:1205`
- **Owner:** Guys
- **Description:** Change this once we change this to the right error
- **Priority:** Medium

### 7. Delete Tag Error Alignment
- **File:** `pkg/graveler/ref/manager.go:536`
- **Owner:** Unassigned (Issue 3640)
- **Description:** Align with delete tag DB - return ErrNotFound when tag does not exist
- **Priority:** Medium

---

## Feature Enhancements

### 1. Request Customization
- **File:** `esti/adapter_utils.go:100`
- **Owner:** ariels
- **Description:** Allow customization of request
- **Priority:** Low

### 2. Add RunOptions/HostConfigs to Docker Test
- **File:** `pkg/dockertest/lakefs_container.go:80`
- **Owner:** ariels
- **Description:** Add RunOptions? Add HostConfigs?
- **Priority:** Low

### 3. Range Manager Iterator
- **File:** `pkg/graveler/sstable/range_manager.go:82`
- **Owner:** ariels
- **Description:** reader.NewIter(lookup, lookup)?
- **Priority:** Low

### 4. Async Timeout and Cancel Logic
- **File:** `cmd/lakectl/cmd/async.go:43`
- **Owner:** niro
- **Description:** Implement timeout and cancel logic here
- **Priority:** High

### 5. Glue Additional Input Params
- **File:** `pkg/actions/lua/storage/aws/glue.go:172`
- **Owner:** isan
- **Description:** Additional input params: partition index and iceberg table format
- **Priority:** Medium

### 6. Link Underline in Login
- **File:** `cmd/lakectl/cmd/login.go:24`
- **Owner:** ariels
- **Description:** Underline the link?
- **Priority:** Low

### 7. Copy Options Counter
- **File:** `pkg/gateway/operations/putobject.go:312`
- **Owner:** ariels
- **Description:** Add a counter for how often a copy has different options
- **Priority:** Low

### 8. GetObject Full S3 API Implementation
- **File:** `pkg/gateway/operations/getobject.go:91`
- **Owner:** Unassigned
- **Description:** Implement the rest of S3 GetObject API
- **Priority:** Medium

### 9. TierFS Configurability
- **File:** `pkg/pyramid/params/params.go:69`
- **Owner:** itai
- **Description:** Make this configurable for more than 2 TierFS instances
- **Priority:** Medium

### 10. Authenticator ID
- **File:** `pkg/auth/authenticator.go:42`
- **Owner:** ariels
- **Description:** Add authenticator ID here
- **Priority:** Low

### 11. Auth Service Swap Support
- **File:** `pkg/auth/basic_service.go:237`
- **Owner:** niro
- **Description:** Support swap?
- **Priority:** Low

---

## Architecture / Refactoring

### 1. Config OIDC Consolidation
- **File:** `pkg/config/config.go:746`
- **Owner:** isan
- **Description:** Consolidate with OIDC
- **Priority:** Medium

### 2. Struct Keys Semantics
- **File:** `pkg/config/struct_keys.go:97`
- **Owner:** ariels
- **Description:** Possible to add, but need to define the semantics
- **Priority:** Low

### 3. Auth Middleware DB Handling Consolidation
- **File:** `pkg/api/auth_middleware.go:251`
- **Owner:** isan
- **Description:** Consolidate userFromOIDC and userFromSAML below here internal db handling code
- **Priority:** Medium

### 4. PR Logic Service Extraction (1)
- **File:** `pkg/graveler/ref/manager.go:720`
- **Owner:** niro
- **Description:** Move all the PR logic into a dedicated service similar to actions
- **Priority:** Medium

### 5. PR Logic Service Extraction (2)
- **File:** `pkg/graveler/ref/manager.go:721`
- **Owner:** niro
- **Description:** For now we put all the logic here under a single block
- **Priority:** Medium

### 6. Graveler Return Error on Conflicts Only
- **File:** `pkg/graveler/graveler.go:1430`
- **Owner:** Guys
- **Description:** Return error only on conflicts, currently returns error for any changes on staging
- **Priority:** Medium

### 7. Parallelize Get from Tokens
- **File:** `pkg/graveler/graveler.go:1765`
- **Owner:** Unassigned
- **Description:** In most cases used by Get flow, assuming key is usually found in committed, need to parallelize the get from tokens
- **Priority:** High

### 8. Update Fields Instead of Fail
- **File:** `pkg/graveler/graveler.go:2426`
- **Owner:** eden (Issue 3586)
- **Description:** If the branch commit id hasn't changed, update the fields instead of fail
- **Priority:** Medium

### 9. Verify Staging Empty
- **File:** `pkg/graveler/graveler.go:2596`
- **Owner:** ariels
- **Description:** Up to here. Verify staging is empty!
- **Priority:** Medium

### 10. Background Delete Process
- **File:** `pkg/graveler/ref/manager.go:347`
- **Owner:** niro
- **Description:** This should be a background delete process
- **Priority:** High

### 11. Request ID Retrieval
- **File:** `pkg/graveler/ref/manager.go:449`
- **Owner:** ariels
- **Description:** Get request ID in a nicer way
- **Priority:** Low

### 12. List Objects Prefix Logic
- **File:** `pkg/gateway/operations/listobjects.go:144`
- **Owner:** Unassigned
- **Description:** Same prefix logic also in V1!
- **Priority:** Medium

---

## Bug Fixes / Known Issues

### 1. SST Files Generator Issue
- **File:** `clients/spark/src/test/resources/parser-test/sst_files_generator.go:249`
- **Owner:** Tals (Issue 2419)
- **Description:** Remove after resolving issue 2419
- **Priority:** Medium

### 2. S3 Error Encoding
- **File:** `pkg/gateway/errors/errors.go:255`
- **Owner:** ariels
- **Description:** S3 itself encodes the bad header in XML tags ArgumentName
- **Priority:** Low

### 3. Fluffy Issue 320
- **File:** `esti/auth_test.go:317`
- **Owner:** niro
- **Description:** See https://github.com/treeverse/fluffy/issues/320
- **Priority:** Medium

### 4. Gateway Unknown Operation Status
- **File:** `pkg/gateway/handler.go:141`
- **Owner:** johnnyaug
- **Description:** Consider other status code or add text with unknown gateway operation
- **Priority:** Low

### 5. CreationDate Required Check
- **File:** `pkg/api/controller.go:1573`
- **Owner:** barak
- **Description:** Check if CreationDate should be required
- **Priority:** Low

### 6. Action Check
- **File:** `pkg/api/controller.go:3280`
- **Owner:** ozkatz
- **Description:** Can we have another action here?
- **Priority:** Low

### 7. Sanitize Title and Description
- **File:** `pkg/api/controller.go:6114`
- **Owner:** niro
- **Description:** Sanitize title and description!
- **Priority:** High (Security)

### 8. TierFS Logging Fields
- **File:** `pkg/pyramid/tier_fs.go:101`
- **Owner:** ariels
- **Description:** Does this add the fields? (Uses a different logging path...)
- **Priority:** Low

### 9. Block Test Range Determination
- **File:** `pkg/block/blocktest/adapter.go:59`
- **Owner:** niro
- **Description:** End smaller than start behavior to be determined
- **Priority:** Low

### 10. Windows Fuzzing Failure
- **File:** `pkg/local/diff_test.go:564`
- **Owner:** ariels
- **Description:** Fuzzing on Windows will probably fail
- **Priority:** Low

---

## Unimplemented Methods (Fakes/Stubs)

These TODOs indicate unimplemented methods in test fakes:

| File | Line |
|------|------|
| `pkg/graveler/testutil/fakes.go` | 881 |
| `pkg/graveler/testutil/fakes.go` | 886 |
| `pkg/graveler/testutil/fakes.go` | 891 |
| `pkg/block/blocktest/basic_suite.go` | 92 (Test abs paths) |
| `pkg/graveler/validate.go` | 89 (Any other validations?) |

---

## High Priority Items Summary

1. **Security:** Sanitize title and description (`pkg/api/controller.go:6114`)
2. **Memory:** Azure buffer release issue (`pkg/block/azure/chunkwriting.go:136`)
3. **Error Handling:** Distinguish missing repo vs missing key (`pkg/gateway/operations/headobject.go:30`, `pkg/gateway/operations/getobject.go:78`)
4. **Performance:** Parallelize get from tokens (`pkg/graveler/graveler.go:1765`)
5. **Architecture:** Background delete process (`pkg/graveler/ref/manager.go:347`)
6. **Feature:** Implement timeout and cancel logic (`cmd/lakectl/cmd/async.go:43`)

---

## By Owner

| Owner | Count |
|-------|-------|
| ariels | 17 |
| niro | 16 |
| Guys | 6 |
| isan | 3 |
| barak | 2 |
| Unassigned | 12 |
| Others (eden, Tals, itai, johnnyaug, nopcoder, ozkatz) | 6 |
