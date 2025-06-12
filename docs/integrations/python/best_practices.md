---
title: Python SDKs Best Practices & Performance
description: Guidance on choosing the right Python tools for lakeFS, optimizing performance, and common patterns.
parent: Python Integration
grand_parent: Integrations
---

# Python SDKs: Best Practices & Performance Guide

{% raw %}{% include toc.html %}{% endraw %}

This guide provides best practices, performance considerations, and comparative insights for using Python with lakeFS. Choosing the right tool and approach can significantly impact the efficiency and maintainability of your data workflows.

## Choosing the Right Python Tool for the Job

lakeFS offers several Python integration methods, each suited for different needs:

*   **[High-Level Python SDK (`lakefs`)](./high_level_sdk.md)**:
    *   **Use When**: You need an intuitive, Pythonic interface for most common lakeFS operations (repository/branch management, object I/O, commits, merges, transactions). This is the **recommended starting point** for most custom scripting and application development.
    *   **Strengths**: Ease of use, actively developed, good balance of abstraction and control, built-in helper methods (like transactions, import manager).
    *   **Considerations**: Might not expose the absolute newest or most niche API endpoints immediately upon their release compared to the Generated SDK.

*   **[Generated Python SDK (`lakefs_sdk`)](./generated_sdk.md)**:
    *   **Use When**: You require access to the very latest lakeFS API features, need fine-grained control over API calls, or are building custom abstractions on top of the lakeFS API.
    *   **Strengths**: Complete API coverage, direct mapping to REST API calls.
    *   **Considerations**: Less abstracted and can be more verbose for common tasks compared to the High-Level SDK. Requires manual handling of some lower-level details.

*   **[lakefs-spec (`fsspec` compatible)](./python/lakefs_spec.md)**:
    *   **Use When**: You want to integrate lakeFS with `fsspec`-compatible libraries like Pandas, Dask, DuckDB, Polars, etc., using familiar file-system paths (`lakefs://repo/branch/path`).
    *   **Strengths**: Seamless integration with the PyData ecosystem, convenient for data scientists and analysts. Offers features like caching.
    *   **Considerations**: It's a third-party library. Performance for very high-throughput tasks might vary compared to direct SDK usage, depending on the operation and `fsspec` overhead.

*   **[Boto3 (S3 Gateway)](./python/boto3.md)**:
    *   **Use When**: You have existing Python code using Boto3 for S3, or you prefer the Boto3 API for S3-style object operations against lakeFS.
    *   **Strengths**: Familiarity for those experienced with Boto3, allows reuse of existing S3 tooling/scripts for basic object operations.
    *   **Considerations**: Cannot perform lakeFS-specific version control operations (commit, branch, merge, etc.). Performance characteristics are tied to the S3 gateway's translation layer.

**General Recommendation:** Start with the High-Level `lakefs` SDK. If you hit limitations or have specific needs for direct API calls or `fsspec` integration, then explore the Generated SDK or `lakefs-spec` respectively. Use Boto3 primarily for leveraging existing S3-based workflows for object operations.

## Performance Considerations

Optimizing performance when interacting with lakeFS via Python depends on the operation, data size, and chosen tool.

### Object Uploads and Downloads

*   **High-Level SDK (`lakefs`)**:
    *   **Streaming**: For large files, use the streaming capabilities (`object.writer()` and `object.reader()`). This avoids loading the entire file into memory.
    *   **Pre-signed URLs**: The `object.writer(pre_sign=True)` (for uploads) and `object.reader(pre_sign=True)` (for downloads, though less common as direct read is efficient) can sometimes offer performance benefits for very large files by interacting directly with the underlying object store, bypassing the lakeFS server for the data transfer part after the initial setup. This is handled transparently by the SDK.
*   **`lakefs-spec`**:
    *   **Caching**: `lakefs-spec` offers caching mechanisms (`pip install lakefs-spec[cache]`). This can significantly speed up repeated reads of the same data. Refer to the [lakefs-spec caching documentation](https://lakefs-spec.org/latest/guides/caching/) for details.
    *   **Block Size**: For libraries like Pandas reading via `fsspec`, the `blocksize` parameter (or similar, specific to the library) when reading/writing Parquet or CSV files can impact performance. Experiment with different block sizes.
*   **Boto3**:
    *   **Managed Uploader/Downloader**: Boto3's `S3Transfer` (used by `upload_file`, `download_file`) automatically handles multipart uploads/downloads for large files, including parallel transfers. This is generally efficient.
        ```python
        # Example: Boto3 managed upload (often more performant for large files than put_object with a stream)
        # s3_client.upload_file(LocalPath, BucketName, ObjectKey, Config=boto3.s3.transfer.TransferConfig(multipart_threshold=...))
        ```
    *   **Presigned URLs with Boto3**: Generating a presigned URL via lakeFS (e.g., using the High-Level or Generated SDKs to call the lakeFS API for a presigned URL) and then using a simple HTTP library (like `requests`) or Boto3's `put_object`/`get_object` with the presigned URL can be very performant for large data transfers as it goes directly to the object store. This offloads bandwidth from the lakeFS server itself.
        *   The lakeFS server needs to support generating these correctly for the specific operation.
        *   The [High-Level SDK's upload/download with `pre_sign=True`](./high_level_sdk.md#object-operations-io) often abstracts this pattern.
*   **General Tips for Large Files**:
    *   **Parallelism**: If you are processing many independent files, consider using Python's `multiprocessing` or `concurrent.futures` to parallelize SDK calls for different objects. Ensure your client objects are handled correctly across processes if needed (some SDK clients might not be thread/process-safe without careful instantiation).
    *   **Network**: Ensure good network connectivity between your client and the lakeFS server / underlying storage.

### Listing Objects

*   **Pagination**: When listing many objects, always use pagination. All SDKs/tools that list objects provide a way to fetch results in pages. Fetching thousands of object paths in a single request can be slow and memory-intensive.
*   **Prefixes**: Use prefixes as specifically as possible to limit the scope of listing operations.
*   **High-Level SDK vs. Generated SDK**: For listing, the overhead difference is usually minimal; choose based on API preference.
*   **`lakefs-spec`**: `fs.ls()` or `fs.glob()` will also benefit from specific paths.

### Commits and Merges

*   **Minimize Diff Size**: Commits and merges operate on the metadata changes between references. While generally fast, operations on branches with extremely large numbers of changed objects (e.g., hundreds of thousands or millions) will naturally take longer to compute the diff and apply.
*   **Commit Frequency**: For batch ETL jobs, it's generally better to commit once after a logical batch of data is processed rather than committing after every single file. However, avoid making commits too infrequent if it means a very large number of objects change in a single commit. Find a balance.
*   **Transactions (High-Level SDK & `lakefs-spec`)**: For a sequence of operations that must be atomic, transactions are excellent. They do involve creating an ephemeral branch and merging, so for very high-frequency, small atomic updates, there's some overhead. Evaluate if the atomicity guarantee is worth it for your specific use case.

## General Error Handling Patterns

Robust applications should always include error handling.

*   **Specific Exceptions First**: Catch the most specific exceptions before generic ones. For example, with the High-Level SDK, catch `lakefs.exceptions.NotFoundException` before `lakefs.exceptions.LakeFSException`. With Boto3, catch `botocore.exceptions.ClientError` and inspect `error.response['Error']['Code']`.
*   **Retry Mechanisms**: For transient network issues or server-side throttling (if applicable), implement a retry mechanism with exponential backoff and jitter. Some libraries or SDKs might have this built-in for certain operations, but check their documentation.
    ```python
    # Pseudocode for retry
    # attempts = 0
    # max_attempts = 5
    # while attempts < max_attempts:
    #     try:
    #         # SDK call
    #         break # Success
    #     except PotentiallyTransientError as e:
    #         attempts += 1
    #         if attempts == max_attempts:
    #             raise # Max attempts reached
    #         time.sleep(2**attempts + random.uniform(0,1)) # Exponential backoff with jitter
    ```
*   **Logging**: Include detailed logging, especially for errors. Log the type of operation, parameters (be careful with sensitive data), and the error received. This is invaluable for debugging.
*   **Idempotency**: Design operations to be idempotent where possible. If an operation is retried, it shouldn't cause unintended side effects if the original operation partially succeeded. lakeFS operations are generally idempotent (e.g., creating an existing repository with the same parameters won't cause an error or duplicate it).

## Security Best Practices

*   **Credentials Management**:
    *   Avoid hardcoding credentials in scripts.
    *   Use environment variables (`LAKEFS_ACCESS_KEY_ID`, `LAKEFS_SECRET_ACCESS_KEY`, `LAKEFS_SERVER_ENDPOINT` for SDKs; `AWS_ACCESS_KEY_ID`, etc. for Boto3 if using its standard credential chain with lakeFS).
    *   Utilize `lakectl` configuration files (`~/.lakectl.yaml`) which the Python SDKs can automatically pick up.
    *   For applications running in compute environments (like EC2, ECS, Kubernetes), use IAM roles or service accounts if your lakeFS setup supports them for authentication (e.g., via OIDC or STS integration for lakeFS Enterprise).
*   **Permissions**: Follow the principle of least privilege. Grant Python script users/roles only the necessary lakeFS permissions for the tasks they need to perform.
*   **HTTPS**: Always use HTTPS for communication with the lakeFS server to protect data in transit. Ensure SSL/TLS verification is enabled (the default for most clients) unless there's a specific, understood reason for disabling it in a development environment.

By considering these practices, you can build more robust, performant, and secure Python applications that leverage the full power of lakeFS.
---
