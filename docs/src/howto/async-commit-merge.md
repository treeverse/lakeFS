---
title: Asynchronous Commit and Merge
description: Perform large commit and merge operations asynchronously to avoid timeouts and indeterminate outcomes.
---

# Asynchronous Commit and Merge

!!! info
    Available in **lakeFS Enterprise** and **lakeFS Cloud** only.

Asynchronous commit and merge handle large operations that may exceed timeout limits. Operations return immediately with an operation ID that you poll for status, ensuring reliable completion for commits and merges involving millions of objects.

## When to Use

Use async operations when:

- Committing or merging millions of objects that exceed gateway timeout limits
- Operations take longer than your load balancer timeout
- Running in CI/CD pipelines where you need reliable completion status

For standard operations with smaller datasets, synchronous endpoints work well and provide immediate results.

## Prerequisites

- lakeFS Enterprise or lakeFS Cloud
- Same RBAC permissions as synchronous operations (see [Permissions](#permissions))
- When polling programmatically, ability to persist the operation ID

## Using lakectl

lakectl automatically detects whether async operations are available and uses them when supported, falling back to synchronous operations in OSS installations. The command syntax is identical in both cases:

### Commit

```bash
lakectl commit lakefs://example-repo/main \
  --message "Daily data ingestion" \
  --meta "pipeline=etl-prod" \
  --meta "run_id=2024-12-19"
```

### Merge

```bash
lakectl merge lakefs://example-repo/feature-branch lakefs://example-repo/main
```

lakectl handles async operations transparently, polling for completion in the background. Operation IDs are not currently displayed. To access operation IDs and poll status independently, use the [REST API](#using-the-rest-api).

## Using the REST API

### Submitting an Async Commit

**Endpoint:** `POST /repositories/{repository}/branches/{branch}/commits/async`

**Request:**
```bash
curl -X POST "https://lakefs.example.com/api/v1/repositories/example-repo/branches/main/commits/async" \
  -H "Authorization: Bearer ${LAKEFS_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Daily data ingestion",
    "metadata": {
      "pipeline": "etl-prod",
      "run_id": "2024-12-19"
    }
  }'
```

**Response (202 Accepted):**
```json
{
  "id": "op_a1b2c3d4e5f6"
}
```

!!! important "Save the operation ID"
Store the `id` for status queries. The ID is required to retrieve results.

### Checking Commit Status

**Endpoint:** `GET /repositories/{repository}/branches/{branch}/commits/async/{operation_id}/status`

**Request:**
```bash
curl "https://lakefs.example.com/api/v1/repositories/example-repo/branches/main/commits/async/op_a1b2c3d4e5f6/status" \
  -H "Authorization: Bearer ${LAKEFS_ACCESS_TOKEN}"
```

**Response (In Progress):**
```json
{
  "task_id": "op_a1b2c3d4e5f6",
  "completed": false,
  "update_time": "2024-12-19T10:02:15Z"
}
```

**Response (Completed Successfully):**
```json
{
  "task_id": "op_a1b2c3d4e5f6",
  "completed": true,
  "update_time": "2024-12-19T10:05:43Z",
  "result": {
    "id": "c1a2b3c4d5e6f7g8h9i0",
    "parents": ["b1a2b3c4d5e6f7g8h9i0"],
    "committer": "user@example.com",
    "message": "Daily data ingestion",
    "creation_date": 1734604543,
    "meta_range_id": "abc123def456",
    "metadata": {
      "pipeline": "etl-prod",
      "run_id": "2024-12-19"
    },
    "generation": 42,
    "version": 1
  }
}
```

**Response (Failure due to branch protection):**
```json
{
  "task_id": "op_a1b2c3d4e5f6",
  "completed": true,
  "update_time": "2024-12-19T10:04:22Z",
  "error": {
    "message": "cannot commit to protected branch"
  },
  "status_code": 403
}
```

### Submitting an Async Merge

**Endpoint:** `POST /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch}/async`

**Request:**
```bash
curl -X POST "https://lakefs.example.com/api/v1/repositories/example-repo/refs/feature-branch/merge/main/async" \
  -H "Authorization: Bearer ${LAKEFS_ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Merge feature branch into main",
    "metadata": {
      "approver": "tech-lead",
      "ticket": "PROJ-123"
    }
  }'
```

**Response (202 Accepted):**
```json
{
  "id": "op_z9y8x7w6v5u4"
}
```

### Checking Merge Status

**Endpoint:** `GET /repositories/{repository}/refs/{sourceRef}/merge/{destinationBranch}/async/{operation_id}/status`

**Request:**
```bash
curl "https://lakefs.example.com/api/v1/repositories/example-repo/refs/feature-branch/merge/main/async/op_z9y8x7w6v5u4/status" \
  -H "Authorization: Bearer ${LAKEFS_ACCESS_TOKEN}"
```

**Response (Completed Successfully):**
```json
{
  "task_id": "op_z9y8x7w6v5u4",
  "completed": true,
  "update_time": "2024-12-19T14:35:12Z",
  "result": {
    "reference": "c5d6e7f8g9h0i1j2k3l4"
  }
}
```

**Response (Failure due to merge conflict):**
```json
{
  "task_id": "op_z9y8x7w6v5u4",
  "completed": true,
  "update_time": "2024-12-19T14:32:45Z",
  "error": {
    "message": "Merge conflict detected between source and destination branches"
  },
  "status_code": 409
}
```

## How It Works

Async operations follow a two-step pattern:

1. **Submit the operation** - Returns immediately with an `id` (HTTP 202 Accepted)
2. **Poll for status** - Check operation status until completion

lakectl handles this workflow automatically. When using the API directly, you're responsible for polling.

## Understanding Operation States

Async operations progress through several states:

| State | Description | completed | Result Available |
|-------|-------------|-----------|------------------|
| `pending` | Operation submitted and queued, waiting for processing | `false` | No |
| `running` | Operation is actively executing | `false` | No |
| `completed` (success) | Operation finished successfully | `true` | Yes (in `result` field) |
| `completed` (failed) | Operation finished with an error | `true` | No (see `error` field) |
| `expired` | Operation timed out after 20 minutes | `true` | No (see `error` field) |

### State Transitions

```
[submitted] → pending → running → completed (success/failure)
                 ↓         ↓
                 └─────────┴──→ expired (after 20 minutes)
```

Operations expire if `update_time` hasn't changed for 20 minutes, regardless of whether they're pending in the queue or actively running.

### The `completed` Field

The `completed` boolean indicates whether the operation has finished, regardless of success or failure:

- `completed: false` - Operation is still in progress (`pending` or `running`)
- `completed: true` - Operation has finished (check `result` or `error` for outcome)

!!! warning "Operation ID Retention: 24 Hours"
    Operation results are retained for **24 hours** after completion. After this window, the operation ID becomes invalid and status queries return 404. Always retrieve and store results promptly after completion.

## Polling Strategy

### Recommended Approach

Use exponential backoff to balance responsiveness with server load:

```python
import time

def poll_operation(get_status_func, max_wait=30):
    """Poll operation status with exponential backoff"""
    wait_time = 1  # Start with 1 second

    while True:
        status = get_status_func()

        if status.completed:
            return status

        time.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait)  # Cap at max_wait seconds
```

### Polling Intervals

- **Initial poll**: 1 second after submission
- **Subsequent polls**: Double the interval (1s → 2s → 4s → 8s → ...)
- **Maximum interval**: 30 seconds
- **Timeout handling**: Operations expire after 20 minutes if not completed

!!! tip "Efficient Polling"
Start with short intervals for quick operations, then back off for longer tasks. This provides fast feedback while reducing server load.

## Timeouts and Expiration

### Operation Timeout

Async operations have a **20-minute timeout**. If an operation doesn't complete within 20 minutes, it will be marked as expired.

**How expiration works:**

When you query the status endpoint, it checks if `update_time` is older than 20 minutes. If yes, the endpoint computes and returns an expired status on-the-fly (the stored task data is never modified to reflect expiration).

**Expired operation response:**
```json
{
  "task_id": "op_abc123",
  "completed": true,
  "update_time": "2024-12-19T10:22:00Z",
  "error": {
    "message": "Operation expired"
  },
  "status_code": 408
}
```

## Error Handling

Async operations return the same error codes as their synchronous counterparts. See the [Commit and Merge documentation](../../quickstart/commit-and-merge/) for details on these standard errors.

### Async-Specific Error Codes

| HTTP Status | Meaning | Description |
|-------------|---------|-------------|
| `408` | Request Timeout | Operation expired (exceeded 20-minute timeout) |
| `501` | Not Implemented | Async operations not available (OSS version) |

**Operation Expired (408):**
```json
{
  "task_id": "op_abc123",
  "completed": true,
  "update_time": "2024-12-19T10:22:00Z",
  "error": {
    "message": "Operation expired"
  },
  "status_code": 408
}
```

Resolution: The operation may have completed but wasn't polled in time, or the operation legitimately timed out. For very large operations, consider breaking them into smaller chunks.

**Not Implemented (501):**

When attempting to use async endpoints on lakeFS OSS, you'll receive a 501 response from the initial POST request (not from the status endpoint).

Resolution: You're using lakeFS OSS. Upgrade to lakeFS Enterprise or use the synchronous commit/merge endpoints.

## Best Practices

### 1. Always Persist Operation IDs

Store operation IDs in your application state, database, or CI/CD pipeline variables so you can query status even if your process restarts. For example, you could store them in Redis, a database, or environment variables in your CI/CD system.

### 2. Implement Timeout Handling

Set your own application-level timeout if you need faster failure detection. Track the elapsed time since submission and exit the polling loop if your timeout threshold is exceeded.

### 3. Use Appropriate Polling Intervals

Balance between responsiveness and server load:

- **Quick operations** (<1 min expected): Poll every 1-2 seconds initially
- **Medium operations** (1-5 min expected): Start at 2s, back off to 10s
- **Large operations** (>5 min expected): Start at 5s, back off to 30s

### 4. Handle Network Failures Gracefully

Network interruptions don't affect the async operation itself. Implement retry logic with exponential backoff when polling the status endpoint to handle transient network issues.

### 5. Log Operation IDs

Include operation IDs in your application logs for troubleshooting. Log when operations are submitted and when they complete to help track down issues in production.

## Permissions

Async operations require the same RBAC permissions as their synchronous counterparts. See [Commit and Merge documentation](../../quickstart/commit-and-merge/) for the full list of required permissions.

The status endpoints also require these same permissions, as they check authorization against the original operation's repository and branch.

## Limitations

- **No cancellation**: Once submitted, async operations cannot be canceled. They will run to completion or timeout.
- **Enterprise only**: Async operations are not available in lakeFS OSS.
- **No SDK wrapper yet**: High-level SDK support is planned but not yet available. Use the REST API directly for now.
- **Concurrent operations**: Multiple operations can be submitted to the same branch and will run in parallel. They coordinate through branch locking, which may cause temporary lock errors under heavy contention. See [Branch Lock Contention](#branch-lock-contention) in Troubleshooting for details.

## Troubleshooting

### Operation Not Found

**Symptom:** Status endpoint returns 404 for a valid operation ID.

**Possible causes:**
- Operation ID was mistyped
- More than 24 hours have passed since completion
- Operation was submitted to a different repository or branch

**Solution:** Verify the operation ID and branch, and ensure you're querying within the 24-hour retention window.

### Operation Stuck in Pending

**Symptom:** Operation status remains `pending` for an unusually long time.

**Possible causes:**
- lakeFS server is under heavy load
- Database connectivity issues
- Worker processes not running

**Solution:** Check lakeFS server logs and monitoring dashboards. Contact your lakeFS administrator if the issue persists.

### Branch Lock Contention

**Symptom:** Operation fails with HTTP 500 and message "branch is currently locked, try again later"

**Cause:** Multiple concurrent operations are trying to modify the same branch. The system automatically retries internally, but if contention persists, the operation fails.

**Solution:**
- Implement retry logic with exponential backoff in your application
- Consider spacing out operations to the same branch
- For batch operations, process them serially rather than in parallel

**Example retry logic:**
```python
import time

def submit_with_retry(submit_func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return submit_func()
        except requests.HTTPError as e:
            if e.response.status_code == 500 and "locked" in e.response.text.lower():
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    time.sleep(wait_time)
                    continue
            raise
    raise Exception("Max retries exceeded")
```

### Unexpected Timeout

**Symptom:** Operation expires before expected, even for moderate-sized commits.

**Possible causes:**
- Hooks taking too long to execute
- Database performance issues
- Network latency to object storage

**Solution:**
- Review hook execution times
- Optimize database queries and indexes
- Check object storage latency metrics

## Related Documentation

- [Commit and Merge (Quickstart)](../../quickstart/commit-and-merge/)
- [Merge Strategies](../../understand/how/merge/)
- [Branch Protection](../protect-branches/)
- [Role-Based Access Control (RBAC)](../../security/rbac/)
- [lakeFS API Reference](../../reference/api/)
