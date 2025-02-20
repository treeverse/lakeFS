package stats

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// UsageCounter is a counter that can be used to track usage of a resource api/gtw
type UsageCounter struct {
	count atomic.Int64
}

var (
	// usageCounters is a list of all usage counters.
	usageCounters []*UsageCounter

	// DefaultUsageReporter is the default usage reporter. It does nothing.
	DefaultUsageReporter UsageReporterOperations = &NopUsageReporter{}

	ErrInvalidUsageKeyFormat = errors.New("invalid usage key format")
)

// NewUsageCounter creates a new usage counter and adds it to the global list of usage counters
func NewUsageCounter() *UsageCounter {
	uc := &UsageCounter{}
	usageCounters = append(usageCounters, uc)
	return uc
}

// Add adds delta to the usage counter
func (uc *UsageCounter) Add(delta int64) {
	uc.count.Add(delta)
}

// Load returns the current value of the usage counter
func (uc *UsageCounter) Load() int64 {
	return uc.count.Load()
}

// Reset resets the usage counter to 0 and returns the previous value
func (uc *UsageCounter) Reset() int64 {
	return uc.count.Swap(0)
}

// Unregister removes the usage counter from the global list of usage counters.
// This is used for testing.
func (uc *UsageCounter) Unregister() {
	usageCounters = slices.DeleteFunc(usageCounters, func(counter *UsageCounter) bool {
		return counter == uc
	})
}

// UsageCounters returns a list of all usage counters.
// This is used for testing.
func UsageCounters() []*UsageCounter {
	return usageCounters
}

const kvUsagePartition = "usage"

// UsageReporter is a usage reporter that persists usage counters to a storage backend.
type UsageReporter struct {
	installationID string
	storage        kv.Store
}

// NopUsageReporter is a usage reporter that does nothing.
type NopUsageReporter struct{}

func (n *NopUsageReporter) Records(_ context.Context) ([]*UsageRecord, error) {
	return nil, nil
}

func (n *NopUsageReporter) Flush(_ context.Context) (time.Time, error) {
	return time.Time{}, nil
}

func (n *NopUsageReporter) InstallationID() string {
	return ""
}

// UsageRecord is a record of usage for a specific month
type UsageRecord struct {
	Year  int
	Month int
	Count int64
}

func NewUsageReporter(installationID string, storage kv.Store) *UsageReporter {
	return &UsageReporter{
		installationID: installationID,
		storage:        storage,
	}
}

func (u *UsageReporter) InstallationID() string {
	return u.installationID
}

// Start starts the usage reporting loop. It persists the current usage
// counters to the storage backend every interval.
// To stop the loop, cancel the context.
func (u *UsageReporter) Start(ctx context.Context, interval time.Duration, logger logging.Logger) {
	if interval == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := u.Flush(ctx); err != nil {
					logger.WithError(err).Error("failed to persist usage counters")
				}
			}
		}
	}()
}

// Records returns all usage records from the storage backend.
// It returns an error if the storage backend fails to scan the records or parse them.
// It will filter out records that are not from the current installation ID.
func (u *UsageReporter) Records(ctx context.Context) ([]*UsageRecord, error) {
	// get all usage records
	// Scan(ctx context.Context, partitionKey []byte, options ScanOptions) (EntriesIterator, error)
	iter, err := u.storage.Scan(ctx, []byte(kvUsagePartition), kv.ScanOptions{})
	if err != nil {
		return nil, err
	}
	var records []*UsageRecord
	for iter.Next() {
		// parse key
		ent := iter.Entry()
		installationID, year, month, err := parseUsageKey(ent.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse usage record '%s': %w", ent.Key, err)
		}
		// filter out records that are not from the current installation ID
		if installationID != u.installationID {
			continue
		}
		// parse value
		count, err := strconv.ParseInt(string(ent.Value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse usage record '%s': %w", ent.Key, err)
		}
		// add record
		records = append(records, &UsageRecord{
			Year:  year,
			Month: month,
			Count: count,
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

var regexpUsageKey = regexp.MustCompile(`^usage/monthly/([^/]+)/(\d{4})(\d{2})$`)

// parseUsageKey parses a usage key into installation ID, year and month.
func parseUsageKey(key []byte) (installationsID string, year int, month int, err error) {
	m := regexpUsageKey.FindSubmatch(key)
	if m == nil {
		return "", 0, 0, fmt.Errorf("%w '%s'", ErrInvalidUsageKeyFormat, key)
	}
	installationsID = string(m[1])
	year, err = strconv.Atoi(string(m[2]))
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid usage key '%s': %w", key, err)
	}
	month, err = strconv.Atoi(string(m[3]))
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid usage key '%s': %w", key, err)
	}
	return installationsID, year, month, nil
}

// formatUsageKey formats a usage key for the given installation ID, year and month.
func formatUsageKey(installationID string, year, month int) []byte {
	return []byte(fmt.Sprintf("usage/monthly/%s/%04d%02d", installationID, year, month))
}

// persistUsageCounters persists the current usage counters to the storage
// backend. Start calls this periodically.
// The counter will be reset to 0 after persisting.
func (u *UsageReporter) persistUsageCounters(ctx context.Context, tm time.Time) error {
	// collect usage counters
	var count int64
	for _, uc := range usageCounters {
		count += uc.Reset()
	}
	if count == 0 {
		return nil
	}
	// update usage record
	record := &UsageRecord{
		Year:  tm.Year(),
		Month: int(tm.Month()),
		Count: count,
	}
	return u.updateRecord(ctx, record)
}

func (u *UsageReporter) updateRecord(ctx context.Context, rec *UsageRecord) error {
	// format the key we use to store the usage record
	key := formatUsageKey(u.installationID, rec.Year, rec.Month)

	// use a backoff with jitter to retry on predicate failures
	const updateRecordRetryDuration = 200 * time.Millisecond
	bo := NewConstantWithJitterBackOff(updateRecordRetryDuration)
	return backoff.Retry(func() error {
		// get current value if found
		var predicate kv.Predicate = nil
		valueWithPredicate, err := u.storage.Get(ctx, []byte(kvUsagePartition), key)
		if err != nil {
			// ignore not found error, we'll create a new record
			if !errors.Is(err, kv.ErrNotFound) {
				return backoff.Permanent(err)
			}
		} else {
			predicate = valueWithPredicate.Predicate
		}

		// updated value (calls + previous calls)
		totalCount := rec.Count
		if valueWithPredicate != nil {
			curr, err := strconv.ParseInt(string(valueWithPredicate.Value), 10, 64)
			if err != nil {
				return backoff.Permanent(fmt.Errorf("failed to parse usage record '%s': %w", key, err))
			}
			totalCount += curr
		}

		// save the updated value
		totalCountStr := strconv.FormatInt(totalCount, 10)
		err = u.storage.SetIf(ctx, []byte(kvUsagePartition), key, []byte(totalCountStr), predicate)
		if err != nil {
			if errors.Is(err, kv.ErrPredicateFailed) {
				return err // retry if predicate failed
			}
			return backoff.Permanent(err)
		}
		return nil
	}, backoff.WithContext(bo, ctx))
}

func (u *UsageReporter) Flush(ctx context.Context) (time.Time, error) {
	tm := time.Now()
	err := u.persistUsageCounters(ctx, tm)
	return tm, err
}

// UsageReporterOperations is an interface for usage reporting.
// The main two implementations here are UsageReporter and NopUsageReporter.
type UsageReporterOperations interface {
	InstallationID() string
	Records(ctx context.Context) ([]*UsageRecord, error)
	Flush(ctx context.Context) (time.Time, error)
}

// ConstantWithJitterBackOff is a backoff strategy that returns a constant interval
// with a random jitter which is half the interval.
type ConstantWithJitterBackOff struct {
	Interval time.Duration
	Jitter   time.Duration
}

func (b *ConstantWithJitterBackOff) Reset() {}
func (b *ConstantWithJitterBackOff) NextBackOff() time.Duration {
	// time.Duration is int64, safe
	return b.Interval + time.Duration(rand.Int63n(int64(b.Interval)))/2 //nolint:gosec
}

func NewConstantWithJitterBackOff(d time.Duration) *ConstantWithJitterBackOff {
	return &ConstantWithJitterBackOff{Interval: d}
}
