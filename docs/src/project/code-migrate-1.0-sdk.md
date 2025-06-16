---
title: Migrating to 1.0
description: Code migration guide detailing API and SDK upgrades, deprecated and new API operations, and SDK migration processes for both Java/JVM and Python with code refactoring examples for improved stability and compatibility.
---

# lakeFS 1.0 - Code Migration Guide

Version 1.0.0 promises API and SDK stability. By "API" we mean any access to a lakeFS REST endpoint. By "SDK" we mean auto-generated lakeFS clients: `lakefs-sdk` for Python and `io.lakefs:sdk` for Java. This guide details the steps to allow you to upgrade your code to enjoy this stability.

Avoid using APIs and SDKs labeled as `experimental`, `internal`, or `deprecated`. If you must use them, be prepared to adjust your application to align with any lakeFS server updates.

Your software developed without such APIs should be compatible with all minor version updates of the lakeFS server from the version you originally developed with.

If you rely on a publicly released API and SDK, it will adhere to semantic versioning. Transitioning your application to a minor SDK version update should be smooth.

The operation names and tags from the [`api/swagger.yml`](https://github.com/treeverse/lakeFS/blob/7d9feeb0211a637e2b8a63abaa629efc968d7c9e/api/swagger.yml) specification might differ based on the SDK or coding language in use.

### Deleted API Operations

The following API operations have been removed:

- `updatePassword`
- `forgotPassword`
- `logBranchCommits`
- `expandTemplate`
- `createMetaRange`
- `ingestRange`
- `updateBranchToken`

### Internal API Operations

The following operations are for `internal` use only and should not be used in your application code. Some deprecated operations have alternatives provided.

- `setupCommPrefs`
- `getSetupState`
- `setup`
- `getAuthCapabilities`
- `uploadObjectPreflight`
- `setGarbageCollectionRulesPreflight`
- `createBranchProtectionRulePreflight`
- `postStatsEvents`
- `dumpRefs` (will be replaced with a long-running API later)
- `restoreRefs` (will be replaced with a long-running API later)
- `createSymlinkFile` (Deprecated)
- `getStorageConfig` (Deprecated. Alternative: `getConfig`)
- `getLakeFSVersion` (Deprecated. Alternative: `getConfig`)
- `stageObject` (Deprecated. Alternatives: `get/link physical address` or `import`)
- `internalDeleteBranchProtectionRule` (Deprecated. Temporary backward support. Alternative: `setBranchProtectionRules`)
- `internalCreateBranchProtectionRule` (Deprecated. Temporary backward support. Alternative: `setBranchProtectionRules`)
- `internalGetBranchProtectionRule` (Deprecated. Temporary backward support. Alternative: `getBranchProtectionRules`)
- `internalDeleteGarbageCollectionRules` (Deprecated. Temporary backward support. Alternative: `deleteGCRules`)
- `internalSetGarbageCollectionRules` (Deprecated. Temporary backward support. Alternative: `setGCRules`)
- `internalGetGarbageCollectionRules` (Deprecated. Temporary backward support. Alternative: `getGCRules`)
- `prepareGarbageCollectionCommits`
- `getGarbageCollectionConfig`

### New/Updated API Operations

Here are the newly added or updated operations:

- `getConfig` (Retrieve lakeFS version and storage info)
- `setBranchProtectionRules` (Route updated)
- `getBranchProtectionRules` (Route updated)
- `getGCRules` (New route introduced)
- `setGCRules` (New route introduced)
- `deleteGCRules` (New route introduced)
- `importStatus` (Response structure updated: 'ImportStatusResp' to 'ImportStatus')
- `uploadObject` (Parameters 'if-none-match' and 'storageClass' are now deprecated)
- `prepareGarbageCollectionCommits` (Request body removed)
- `getOtfDiffs` & `otfDiff` (Removed from 'otf diff' tag; retained in 'experimental' tag)

## Migrating SDK Code for Java and JVM-based Languages

### Introduction

If you are using the lakeFS client for Java or for any other JVM-based language, be aware that the current package is not stable with respect to minor version upgrades.
Transitioning from `io.lakefs:lakefs-client` to `io.lakefs:sdk` will necessitate rewriting your API calls to fit the new design paradigm.


### Problem with the Old Style

Previously, API calls required developers to pass all parameters, including optional ones, in a single function call. As demonstrated in this older style:

```java
ObjectStats objectStat = objectsApi.statObject(
    objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(),
    false, false);
```

This method posed a couple of challenges:

1. **Inflexibility with Upgrades:** If an optional parameter were introduced in newer versions, existing code would fail to compile.
2. **Maintenance Difficulty:** Long argument lists can be challenging to manage and understand, leading to potential mistakes and readability issues.

Adopting the Fluent Style

In the revised SDK, API calls adopt a fluent style, making the code more modular and adaptive to changes.

Here's an example of the new style:

```java
ObjectStats objectStat = objectsApi
    .statObject(
        objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath()
    )
    .userMetadata(true)
    .execute();
```


### Here's a breakdown of the changes:

1. **Initial Function Call:** Begin by invoking the desired function with all required parameters.
2. **Modifying Optional Parameters:** Chain any modifications to optional parameters after the initial function. For instance, `userMetadata` is changed in the example above.
3. **Unused Optional Parameters**: You can safely ignore these. For instance, this code ignores the `presign` optional parameter because it never uses it.
4. **Execution:** Complete the call with the `.execute()` method.

This new design offers several advantages:

- **Compatibility with Upgrades:** When a new optional parameter is introduced, existing code will use its default value, preserving compatibility with minor server version upgrades.
- **Improved Readability:** The fluent style makes it evident which parameters are required and which ones are optional, enhancing code clarity.

When migrating your code, ensure you refactor all your API calls to adopt the new fluent style. This ensures that your application remains maintainable and is safeguarded against potential issues arising from minor SDK version upgrades.

For an illustrative example of the transition between styles, you can view the changes made in this pull request: [lakeFS pull request #6529](https://github.com/treeverse/lakeFS/pull/6529/files#diff-4c50b9ac3bf6bfc05e3b6ff0fbe2fd3214f31afb5b449732d90efe5f97f67167R666).


## Migrating SDK Code for Python

### Introduction

If you continue using the Python `lakefs-client` package for lakeFS, it's important to note that the package has reached its end of support with minor version updates.
You need to switch from `lakefs-client` to `lakefs-sdk`, which will require rewriting your API calls.

### Here's a breakdown of the changes:

1. **Modules change**
   - The previous `model` module was renamed to `models`, meaning that `lakefs_client.model` imports should be replaced with `lakefs_sdk.models` imports.
   - The `apis` module in `lakefs_client` is deprecated and no longer supported. To migrate to the new `api` module in `lakefs_sdk`, you should replace all imports of `lakefs_client.apis` with imports of `lakefs_sdk.api`. We still recommend using the `lakefs_sdk.LakeFSClient` class instead of using the `api` module directly. The `LakeFSClient` class provides a higher-level interface to the LakeFS API and makes it easier to use LakeFS in your applications.
2. **`upload_object` API call:** The `content` parameter value passed to the `objects_api.upload_object` method call should be either a `string` containing the path to the uploaded file, or `bytes` of data to be uploaded.
3. **`get_object`** **API call**: The return value of `client.get_object(...)` is a `bytearray` containing the content of the object.
4. `**client.{operation}_api**`**:** The `lakefs-client` package’s `LakeFSClient` class’s deprecation-marked operations (`client.{operation}`) will no longer be available in the `lakefs-sdk` package’s `LakeFSClient` class. In their place, the `client.{operation}_api` should be used.
5. **Minimum Python Version**: 3.7
6. **Fetching results from response objects**: Instead of fetching the required results properties from a dictionary using `response_result.get_property(prop_name)`, the response objects will include domain specific entities, thus referring to the properties in the `results` of the response - `response_result.prop_name`.

   For example, instead of:

   ```python
   response = lakefs_client.branches.diff_branch(repository='repo', branch='main')
   diff = response.results[0] # 'results' is a 'DiffList' object
   path = diff.get_property('path') # 'diff' is a dictionary
   ```

   You should use:

   ```python
   response = lakefs_client.branches_api.diff_branch(repository='repo', branch='main')
   diff = response.results[0] # 'results' is a 'DiffList' object
   path = diff.path # 'diff' is a 'Diff' object
   ```

