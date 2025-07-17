# Design: Control Sustained Load in lakeFS

## Problem Statement

### The Upload Memory Exhaustion Problem

When users upload objects to lakeFS at high rates through the S3 gateway, a specific failure mode occurs:

1. **High Upload Rate**: Users send data to lakeFS faster than S3 can accept it
2. **S3 Throttling**: S3 writes are getting slower, eventually return ConnectionTimedOut or 503 SlowDown errors
3. **Memory Accumulation**: lakeFS continues reading from clients while S3 rejects writes, causing data to buffer in memory
4. **Resource Exhaustion**: Memory usage spikes until the pod is OOM-killed or the node runs out of resources

### The Mechanics of Memory Accumulation

The problem occurs because of a fundamental mismatch:
- **Input Rate**: lakeFS reads from client HTTP connections at network speed
- **Output Rate**: lakeFS writes to S3 at whatever rate S3 allows (throttled)
- **Delta**: The difference accumulates as buffered data in memory

This buffering happens outside the Go heap, likely in OS network buffers, making it harder to detect and control through normal Go memory management.

### Why This Is Critical

In a multi-tenant Kubernetes environment:
- One user's heavy uploads can exhaust memory on a node
- All pods on that node suffer degraded performance
- Other customers' lakeFS installations become collateral damage
- Kubernetes may evict pods, causing service disruptions

### Current Reality

The issue is triggered specifically by:
- Large object uploads through the S3 gateway
- Sustained upload rates that exceed S3's capacity
- S3 prefix-level rate limiting (not visible to lakeFS in advance)

What makes this challenging:
- We cannot predict S3's rate limits (they vary by prefix and time)
- By the time we see ConnectionTimedOut/SlowDown errors, memory accumulation has already begun
- Client retries make the problem worse, not better

## Scope

### In Scope

This design focuses specifically on:
- **S3 UploadObject operations** through the lakeFS S3 gateway
- **Memory exhaustion** caused by buffering upload data when S3 cannot accept it fast enough

### Out of Scope

The following are explicitly excluded from this design:
- Other operations (GetObject, ListObjects, etc.)
- KV (e.g. DynamoDB) throttling
- Network bandwidth issues unrelated to upload buffering
- General rate limiting for all operations
- Multi-part uploads (handled differently in S3)

## Goals

### Primary Goal

**Prevent memory exhaustion during S3 upload throttling** by controlling the rate at which lakeFS reads upload data from clients to match the rate at which S3 can accept it.

### Specific Objectives

1. **Match Input/Output Rates**: Ensure lakeFS reads data from clients at approximately the same rate it can write to S3

2. **Prevent Memory Accumulation**: Stop upload data from accumulating in memory when S3 is throttling

3. **Detect S3 Throttling**: Quickly identify when S3 is returning 503 errors and adjust behavior accordingly

### Success Criteria

- Memory usage remains stable even during S3 throttling events
- No OOM kills or pod evictions due to upload buffering
- Upload operations complete successfully (though potentially slower)
- S3 503 error rate decreases after implementing backpressure

### Non-Goals

- Optimizing upload performance when S3 is not throttling
- Handling multi-part uploads (separate API with different characteristics)
- Solving S3 rate limits (we can only adapt to them)
- Providing upload progress feedback to clients

## Key Design Decisions

### Rate Limiting vs Backpressure

We must choose between two fundamental approaches:

#### Option 1: Rate Limiting with Throttling

Return explicit errors (HTTP 503 SlowDown) to clients when overloaded.

**Pros:**
- Clear signal to clients
- S3-compatible error responses
- Immediate load reduction
- Easier to implement and reason about

**Cons:**
- Triggers client retries, potentially worsening the situation
- Requires well-behaved clients with exponential backoff
- Binary decision (accept/reject) rather than gradual adjustment
- Poor user experience with failed uploads

#### Option 2: Slowdown with Backpressure

Slow down reading from client connections to match S3's acceptance rate.

**Pros:**
- No explicit errors - uploads complete successfully (just slower)
- Natural TCP flow control prevents client from sending too fast
- Self-regulating - automatically adjusts to S3's actual capacity
- No retries, so no additional load

**Cons:**
- Risk of client timeouts if we slow down too much
- More complex to implement correctly
- Harder to debug and monitor
- May hold connections open for long periods

**Recommendation**: Implement **Rate Limiting with Throttling** for the initial phase as it seems like a simpler and more predictable approach to implement.
However, design the system to allow gradual slowdown with backpressure in the future if needed.

### Trigger Mechanisms

Choosing when to activate throttling/backpressure is critical:

#### Option 1: Static Thresholds

Use fixed limits for connections or throughput.

```yaml
limits:
   max_concurrent_uploads: 100
   max_throughput_mb_per_second: 500
```
**Pros:**
- Simple to implement and understand
- Predictable behavior
- Easy to configure
- Easy to communicate to users

**Cons:**
- Doesn't adapt to actual S3 capacity
- May throttle unnecessarily or fail to throttle when needed
- S3 rate limits vary by prefix and can change over time

#### Option 2: Dynamic Ratio-Based
Monitor the ratio of input rate to S3 acceptance rate.

`trigger_ratio = bytes_read_from_clients / bytes_written_to_s3 
if trigger_ratio > 1.5:
   activate_backpressure()`

**Pros:**

- Adapts to actual S3 performance
- More accurate reflection of the real problem
- Handles varying S3 rate limits

**Cons:**

- Complex to calculate accurately
- Requires sliding windows and careful measurement
- May oscillate or react slowly

##### Option 3: Error-Based Detection
Activate when S3 returns specific errors.

**Pros:**

- Direct signal from S3
- No guessing about capacity

**Cons:**

- Too late - by the time we see errors, memory accumulation has begun
- False positives - connection timeouts don't always indicate rate limiting
- S3 errors are not consistent (sometimes connection reset, sometimes 503)

**Recommendation**: Start with **static thresholds** for simplicity, but design the system to allow dynamic ratio-based adjustments later.

#### Trigger Mechanism: Per-Server vs Per-Installation

##### Option 1: Per-Server (Pod) Limits

Each lakeFS pod independently monitors and enforces limits.

**Pros:**
- Simple implementation - no coordination needed
- Fast local decisions
- No single point of failure
- Scales naturally with pod count

**Cons:**
- Uneven load distribution can cause unfair throttling
- No global view of installation health
- Difficult to enforce installation-wide limits
- May throttle one pod while others are idle

##### Option 2: Per-Installation (Global) Limits

Coordinate limits across all pods in an installation.

**Pros:**
- Fair resource allocation across pods
- Global view enables better decisions
- Can enforce installation-wide quotas
- Better matches S3's per-prefix limits

**Cons:**
- Requires coordination mechanism (Redis/etcd)
- Added latency for coordination
- Single point of failure risk
- Complex to implement correctly

**Recommendation**: Start with **per-server** for simplicity, with hooks to add coordination later.

### Scope of Backpressure: All Requests vs UploadObject Only

#### Option 1: UploadObject Only

Apply backpressure only to PUT object operations.

**Pros:**
- Focused on the actual problem (memory from uploads)
- Minimal impact on other operations
- Simpler to test and reason about
- Lower risk of unintended consequences

**Cons:**
- Other operations might also cause issues later
- Inconsistent behavior across API
- Multiple code paths to maintain

#### Option 2: All S3 Gateway Requests

Apply backpressure to all S3 operations.

**Pros:**
- Consistent behavior across all operations
- Future-proof against other memory issues
- Single implementation to maintain
- Protects against unexpected load patterns

**Cons:**
- Critical APIs like health or ListRepositories that are indicative of system health may be affected unless we implement special handling which adds complexity
- May unnecessarily slow down light operations
- Harder to tune for different operation types
- Risk of impacting read-heavy workloads

**Recommendation**: **UploadObject only** initially, with infrastructure to extend later.

### Granularity of Rate Limiting

#### Option 1: Per-User

Track and limit by authenticated user/access key.

**Pros:**
- Fair allocation between users
- Prevents single user from monopolizing resources
- Natural boundary for quotas
- Good for multi-tenant fairness

**Cons:**
- Users might use multiple access keys
- Requires user tracking infrastructure

#### Option 2: Per-Repository

Track limits at the repository level.

**Pros:**
- Matches lakeFS's natural boundaries
- Easier to communicate limits to users than per-prefix approach
- Can tie to repository quotas/tiers

**Cons:**
- Repositories might share the same S3 prefix

#### Option 3: Per-Prefix

Track limits by S3 prefix (bucket + partial key).

**Pros:**
- Directly matches S3's throttling model
- Most accurate prevention of S3 503s
- Natural fit for the problem

**Cons:**
- Complex to implement (dynamic prefix detection)
- Prefixes aren't known in advance
- Hard to explain to users

#### Option 4: Per-Branch

Track limits by repository + branch.

**Pros:**
- Matches lakeFS's data model
- Reasonable granularity
- Users understand branch concept

**Cons:**
- Branch might span multiple S3 prefixes
- Doesn't prevent S3 throttling within a branch

**Recommendation**: Start without any of these for simplicity, but design the system to allow extension to other granularity later.

### Decision Summary

Based on the analysis, I recommend:

1. **Slowdown** (simpler, can change to backpressure later)
2. **Static thresholds initially** (simpler, can add dynamic later)
3. **Per-server implementation** initially (simpler, can add coordination later)
4. **UploadObject operations only** (focused on the actual problem)

This provides a pragmatic starting point that:
- Solves the immediate memory exhaustion problem
- Is simple enough to implement correctly
- Leaves room for future enhancements

## Implementation Approach

### Token Bucket Libraries for Rate Limiting

For implementing rate limiting in Go, we have three main options:

#### golang.org/x/time/rate
- Traditional token bucket with `Allow()`, `Reserve()`, and `Wait()` methods
- Best for complex scenarios with context cancellation support
- **Good for request-based limiting but not designed for bandwidth control**

#### github.com/juju/ratelimit
- Token bucket with built-in Reader/Writer wrappers
- Optimized for high performance (~175ns per operation)
- **Best choice for bandwidth limiting** with native byte-level control
- Can wrap io.Reader directly for upload bandwidth limiting

#### go.uber.org/ratelimit
- Minimal API with single `Take()` method
- Leaky bucket algorithm despite the name
- Best for simple request rate limiting with minimal overhead

**Recommendation**: Use **juju/ratelimit** for bandwidth limiting (bytes/second) and **golang.org/x/time/rate** for request rate limiting, as they're optimized for different use cases.

### S3 Request Rate and Ramp-up Strategy

AWS S3 has specific rate limits and scaling behaviors that must be considered:

- **Write Operations**: 3,500 PUT/COPY/POST/DELETE requests per second per prefix
- **Read Operations**: 5,500 GET/HEAD requests per second per prefix
- **Gradual Scaling Required**: S3 scales gradually over "on the order of minutes"

#### Why Ramp-up Matters

S3's internal infrastructure needs time to scale. Sudden traffic spikes cause 503 "Slow Down" errors while S3 allocates resources. By implementing a ramp-up strategy, we align with S3's scaling behavior and reduce error rates.

#### Phase 1 Approach

For Phase 1, we'll implement the rate limiter with hooks for ramp-up but won't implement the full adaptive behavior:

```go
type BackpressureConfig struct {
    // Static limits for Phase 1
    MaxRequestsPerSecond   float64
    MaxBytesPerSecond     int64
    MaxConcurrentUploads  int
    
    // Ramp-up parameters (for future implementation)
    EnableRampUp          bool     // false for Phase 1
    InitialRequestRate    float64  // e.g., 10% of max
    RampUpDuration        time.Duration
}
```

This allows us to add ramp-up functionality later without redesigning the system.

### Configuration Schema

```yaml
gateways:
  s3:
    backpressure:
      enabled: true
      scope: ["upload_object"]   # or empty for all S3 operations
      granularity: ""            # empty for Phase 1, later: "repository", "user", "prefix"
      coordination: false        # true for global coordination, false for per-server
      
      limits:
        maxRequestsPerSecond: 100
        maxBytesPerSecond: 104857600  # 100MB/s
        maxConcurrentUploads: 50
        
      # Ramp-up configuration (for future phases)
      rampUp:
        enabled: false
        initialRate: 10  # Start at 10% of max
        durationSeconds: 300  # 5 minutes
      
      errorThreshold:
        windowSec: 30  # time window for error rate calculation
        maxErrorRate: 0.05  # 5% error rate triggers backoff
```

### Core Implementation

#### Backpressure Middleware

Add to `pkg/gateway/middleware.go`:

```go
import (
    "golang.org/x/time/rate"
    "github.com/juju/ratelimit"
)

// BackpressureController manages rate limiting and backpressure
type BackpressureController struct {
    // Request rate limiting
    requestLimiter *rate.Limiter
    
    // Bandwidth limiting
    bandwidthBucket *ratelimit.Bucket
    
    // Concurrency control
    uploadSemaphore chan struct{}
    
    // Error tracking
    errorTracker *ErrorRateTracker
    
    config BackpressureConfig
}

func NewBackpressureController(config BackpressureConfig) *BackpressureController {
    return &BackpressureController{
        requestLimiter: rate.NewLimiter(
            rate.Limit(config.MaxRequestsPerSecond),
            int(config.MaxRequestsPerSecond),
        ),
        bandwidthBucket: ratelimit.NewBucketWithRate(
            float64(config.MaxBytesPerSecond),
            config.MaxBytesPerSecond,
        ),
        uploadSemaphore: make(chan struct{}, config.MaxConcurrentUploads),
        errorTracker: NewErrorRateTracker(
            time.Duration(config.ErrorThreshold.WindowSec) * time.Second,
        ),
        config: config,
    }
}

// BackpressureMiddleware creates the middleware function
func BackpressureMiddleware(bp *BackpressureController) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            // Check if this is an upload operation
            if !isUploadOperation(r) || !bp.config.Enabled {
                next.ServeHTTP(w, r)
                return
            }
            
            ctx := r.Context()
            
            // Apply request rate limiting
            if err := bp.requestLimiter.Wait(ctx); err != nil {
                writeS3ErrorResponse(w, ErrSlowDown)
                bp.errorTracker.RecordError()
                return
            }
            
            // Acquire upload slot
            select {
            case bp.uploadSemaphore <- struct{}{}:
                defer func() { <-bp.uploadSemaphore }()
            case <-ctx.Done():
                writeS3ErrorResponse(w, ErrSlowDown)
                bp.errorTracker.RecordError()
                return
            }
            
            // Apply bandwidth limiting to request body
            rateLimitedBody := ratelimit.Reader(r.Body, bp.bandwidthBucket)
            r.Body = io.NopCloser(rateLimitedBody)
            
            // Call next handler
            next.ServeHTTP(w, r)
        })
    }
}
```

#### Modified Upload Handler

Update `pkg/gateway/operations/putobject.go`:

```go
func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, o *PathOperation) {
    // Existing authentication and validation...
    
    // Middleware already rate-limits the body
    // Continue with normal upload processing
    
    // Monitor S3 errors for adaptive behavior (future phase)
    _, err := h.BlockAdapter.Put(ctx, block.ObjectPointer{
        StorageNamespace: o.Repository.StorageNamespace,
        Identifier:       physicalAddress,
    }, r.Body, block.PutOpts{})
    
    if err != nil {
        // Check if this is an S3 throttling error
        if isS3ThrottlingError(err) {
            // Record for error rate tracking
            if bp := GetBackpressureController(ctx); bp != nil {
                bp.errorTracker.RecordError()
            }
        }
        // Handle error...
    }
}

func isS3ThrottlingError(err error) bool {
    // Check for S3 503 SlowDown, ConnectionTimeout, etc...
    if awsErr, ok := err.(awserr.Error); ok {
        switch awsErr.Code() {
        case "SlowDown", "ServiceUnavailable", 
             "RequestTimeout", "ConnectionTimeout":
            return true
        }
    }
    return false
}
```

### Observability

#### Key Metrics

|Metric|Type|Purpose|
|------|---------|--------|
|lakefs_s3_read_rate_bytes_per_second|Gauge|Input rate from clients|
|lakefs_s3_write_rate_bytes_per_second|Gauge|Output rate to S3|
|lakefs_s3_bandwidth_ratio|Gauge|Read/Write ratio|
|lakefs_s3_active_uploads|Gauge|Current concurrent uploads|
|lakefs_s3_rejected_requests|Counter|Requests rejected due to limits|
|lakefs_s3_throttling_errors|Counter|S3 throttling errors by type|
|lakefs_s3_backpressure_engaged|Gauge|1 if backpressure active, 0 otherwise|

### Safety Mechanisms

- **Feature Flag**: Runtime on/off switch for backpressure without a restart
- **Health Check Exemption**: Always allow `/_health` through

### Testing Strategy

#### Unit Tests

- Test rate-limited reader with various bandwidth limits
- Verify "semaphore" behavior under load
- Test error detection and classification

#### Integration Tests

- Simulate S3 throttling responses
- Verify memory usage stays bounded
- Test client timeout behavior

#### Load Tests

### Tasks Breakdown + Timeline

### Success Metrics

- Zero OOM kills due to upload buffering
- S3 503 error rate reduced by > X%
- Memory usage remains stable under a sustained load
- Upload success rate > X% even during S3 throttling
- No impact on non-upload operations