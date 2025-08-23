package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	redis "github.com/redis/go-redis/v9"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

type Driver struct{}

type Store struct {
	client *redis.Client
	params *Params
}

type EntriesIterator struct {
	ctx          context.Context
	partitionKey []byte
	pattern      string
	cursor       uint64
	keys         []string
	currentIndex int
	store        *Store
	err          error
	batchSize    int
	seekKey      []byte
	endReached   bool
}

const (
	DriverName      = "redis"
	DefaultDB       = 0
	DefaultPoolSize = 10
	keyDelimiter    = ":"
)

var (
	ErrInvalidKeyFormat = errors.New("invalid key format")
)

func init() { //nolint:gochecknoinits
	kv.Register(DriverName, &Driver{})
}

type Params struct {
	Address   string
	Password  string
	DB        int
	PoolSize  int
	KeyPrefix string
}

func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	if kvParams.Redis == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}

	params := &Params{
		Address:   kvParams.Redis.Address,
		Password:  kvParams.Redis.Password,
		DB:        kvParams.Redis.DB,
		PoolSize:  kvParams.Redis.PoolSize,
		KeyPrefix: kvParams.Redis.KeyPrefix,
	}

	if params.PoolSize == 0 {
		params.PoolSize = DefaultPoolSize
	}

	client := redis.NewClient(&redis.Options{
		Addr:     params.Address,
		Password: params.Password,
		DB:       params.DB,
		PoolSize: params.PoolSize,
	})

	// Test connection
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrConnectFailed, err)
	}

	return &Store{
		client: client,
		params: params,
	}, nil
}

func (s *Store) formatKey(partitionKey, key []byte) string {
	parts := []string{}
	if s.params.KeyPrefix != "" {
		parts = append(parts, s.params.KeyPrefix)
	}
	parts = append(parts, string(partitionKey), string(key))
	return strings.Join(parts, keyDelimiter)
}

func (s *Store) parseKey(redisKey string) (partitionKey, key []byte, err error) {
	parts := strings.Split(redisKey, keyDelimiter)

	expectedParts := 2
	if s.params.KeyPrefix != "" {
		expectedParts = 3
	}

	if len(parts) != expectedParts {
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidKeyFormat, redisKey)
	}

	if s.params.KeyPrefix != "" {
		return []byte(parts[1]), []byte(parts[2]), nil
	}
	return []byte(parts[0]), []byte(parts[1]), nil
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}

	redisKey := s.formatKey(partitionKey, key)
	val, err := s.client.Get(ctx, redisKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	return &kv.ValueWithPredicate{
		Value:     val,
		Predicate: kv.Predicate(val),
	}, nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	redisKey := s.formatKey(partitionKey, key)
	err := s.client.Set(ctx, redisKey, value, 0).Err()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	redisKey := s.formatKey(partitionKey, key)

	script := `
		local key = KEYS[1]
		local new_value = ARGV[1]
		local expected_value = ARGV[2]
		local mode = ARGV[3]
		
		local current_value = redis.call('GET', key)
		
		if mode == 'nil' then
			-- Insert only if key doesn't exist
			if current_value == false then
				redis.call('SET', key, new_value)
				return 1
			end
			return 0
		elseif mode == 'exists' then
			-- Update only if key exists
			if current_value ~= false then
				redis.call('SET', key, new_value)
				return 1
			end
			return 0
		else
			-- Update only if current value matches expected
			if current_value == expected_value then
				redis.call('SET', key, new_value)
				return 1
			end
			return 0
		end
	`

	var mode string
	var expectedValue string

	switch valuePredicate {
	case nil:
		mode = "nil"
	case kv.PrecondConditionalExists:
		mode = "exists"
	default:
		mode = "match"
		expectedValue = string(valuePredicate.([]byte))
	}

	result, err := s.client.Eval(ctx, script, []string{redisKey}, string(value), expectedValue, mode).Result()
	if err != nil {
		return fmt.Errorf("redis setIf: %w", err)
	}

	if result.(int64) != 1 {
		return kv.ErrPredicateFailed
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}

	redisKey := s.formatKey(partitionKey, key)
	err := s.client.Del(ctx, redisKey).Err()
	if err != nil {
		return fmt.Errorf("redis delete: %w", err)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}

	// Create pattern to match all keys with this partition
	pattern := s.formatKey(partitionKey, []byte("*"))

	batchSize := 100
	if options.BatchSize > 0 {
		batchSize = options.BatchSize
	}

	iterator := &EntriesIterator{
		ctx:          ctx,
		partitionKey: partitionKey,
		pattern:      pattern,
		cursor:       0,
		store:        s,
		batchSize:    batchSize,
		currentIndex: -1,
	}

	if len(options.KeyStart) > 0 {
		iterator.seekKey = options.KeyStart
	}

	// Load first batch
	iterator.loadNextBatch()

	return iterator, iterator.err
}

func (s *Store) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

func (it *EntriesIterator) loadNextBatch() {
	if it.endReached {
		return
	}

	keys, cursor, err := it.store.client.Scan(it.ctx, it.cursor, it.pattern, int64(it.batchSize)).Result()
	if err != nil {
		it.err = fmt.Errorf("redis scan: %w", err)
		return
	}

	it.cursor = cursor
	if cursor == 0 {
		it.endReached = true
	}

	// Filter keys and extract sortable key parts
	type keyEntry struct {
		redisKey string
		keyPart  []byte
	}

	filteredEntries := make([]keyEntry, 0, len(keys))
	for _, key := range keys {
		_, keyPart, err := it.store.parseKey(key)
		if err != nil {
			continue // Skip malformed keys
		}

		// Apply seek filter if needed
		if it.seekKey != nil && bytes.Compare(keyPart, it.seekKey) < 0 {
			continue
		}

		filteredEntries = append(filteredEntries, keyEntry{
			redisKey: key,
			keyPart:  keyPart,
		})
	}

	// Sort by key part using a proper sorting algorithm
	if len(filteredEntries) > 1 {
		// Simple bubble sort for now - could be optimized with sort.Slice
		for i := 0; i < len(filteredEntries)-1; i++ {
			for j := i + 1; j < len(filteredEntries); j++ {
				if bytes.Compare(filteredEntries[i].keyPart, filteredEntries[j].keyPart) > 0 {
					filteredEntries[i], filteredEntries[j] = filteredEntries[j], filteredEntries[i]
				}
			}
		}
	}

	// Extract sorted keys
	it.keys = make([]string, len(filteredEntries))
	for i, entry := range filteredEntries {
		it.keys[i] = entry.redisKey
	}

	it.currentIndex = -1
}

func (it *EntriesIterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.currentIndex++

	// If we've reached the end of current batch, try to load next batch
	for it.currentIndex >= len(it.keys) {
		if it.endReached {
			return false
		}
		it.loadNextBatch()
		if it.err != nil {
			return false
		}
		if len(it.keys) == 0 && it.endReached {
			return false
		}
		if len(it.keys) > 0 {
			it.currentIndex = 0
			break
		}
		// Continue to next batch if this one is empty but we haven't reached the end
	}

	return it.currentIndex < len(it.keys)
}

func (it *EntriesIterator) SeekGE(key []byte) {
	it.seekKey = key
	it.cursor = 0
	it.endReached = false
	it.loadNextBatch()

	// Find the first key >= seekKey in current batch
	for i, redisKey := range it.keys {
		_, keyPart, err := it.store.parseKey(redisKey)
		if err != nil {
			continue
		}
		if bytes.Compare(keyPart, key) >= 0 {
			it.currentIndex = i - 1 // Will be incremented by Next()
			return
		}
	}

	// If not found in current batch, position at end
	it.currentIndex = len(it.keys) - 1
}

func (it *EntriesIterator) Entry() *kv.Entry {
	if it.err != nil || it.currentIndex < 0 || it.currentIndex >= len(it.keys) || len(it.keys) == 0 {
		return nil
	}

	redisKey := it.keys[it.currentIndex]
	partitionKey, key, err := it.store.parseKey(redisKey)
	if err != nil {
		it.err = err
		return nil
	}

	val, err := it.store.client.Get(it.ctx, redisKey).Result()
	if errors.Is(err, redis.Nil) {
		// Key was deleted between scan and get - skip it
		return nil
	}
	if err != nil {
		it.err = fmt.Errorf("redis get during iteration: %w", err)
		return nil
	}

	return &kv.Entry{
		PartitionKey: partitionKey,
		Key:          key,
		Value:        []byte(val),
	}
}

func (it *EntriesIterator) Err() error {
	return it.err
}

func (it *EntriesIterator) Close() {
	it.keys = nil
	it.currentIndex = -1
	it.err = kv.ErrClosedEntries
}
