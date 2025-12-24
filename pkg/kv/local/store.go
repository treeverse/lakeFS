package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/badger/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

func partitionRange(partitionKey []byte) []byte {
	result := make([]byte, len(partitionKey)+1)
	copy(result, partitionKey)
	result[len(result)-1] = kv.PathDelimiter[0]
	return result
}

func composeKey(partitionKey, key []byte) []byte {
	pr := partitionRange(partitionKey)
	result := make([]byte, len(pr)+len(key))
	copy(result, pr)
	copy(result[len(pr):], key)
	return result
}

type Store struct {
	db           *badger.DB
	logger       logging.Logger
	prefetchSize int
	refCount     int
	path         string
}

var _ kv.TransactionerStore = (*Store)(nil)

func getFromTxn(ctx context.Context, log logging.Logger, txn *badger.Txn, partitionKey, key []byte) ([]byte, error) {
	log = log.WithContext(ctx).WithFields(logging.Fields{
		"partition": string(partitionKey),
		"key":       string(key),
		"op":        "get",
	})

	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return nil, kv.ErrMissingKey
	}

	start := time.Now()
	k := composeKey(partitionKey, key)
	log.Trace("performing operation")
	item, err := txn.Get(k)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		log.WithError(err).Error("error getting key")
		return nil, err
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		err = fmt.Errorf("extract key: %w", err)
		log.WithError(err).Error("operation failed")
		return nil, err
	}
	log.WithField("took", time.Since(start)).WithError(err).WithField("size", len(value)).Trace("operation complete")
	return value, nil
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		value, err = getFromTxn(ctx, s.logger, txn, partitionKey, key)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &kv.ValueWithPredicate{
		Value:     value,
		Predicate: kv.Predicate(value),
	}, nil
}

func setFromTxn(ctx context.Context, log logging.Logger, txn *badger.Txn, partitionKey, key, value []byte) error {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log = log.WithFields(logging.Fields{
		"key": string(k),
		"op":  "set",
	}).WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return kv.ErrMissingKey
	}
	if value == nil {
		log.WithError(kv.ErrMissingValue).Warn("got nil value")
		return kv.ErrMissingValue
	}
	err := txn.Set(k, value)
	if err != nil {
		log.WithError(err).Error("error setting value")
		return err
	}
	log.WithField("took", time.Since(start)).Trace("done setting value")
	return nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return setFromTxn(ctx, s.logger, txn, partitionKey, key, value)
	})
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log := s.logger.WithField("key", string(k)).WithField("op", "set_if").WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return kv.ErrMissingKey
	}
	if value == nil {
		log.WithError(kv.ErrMissingValue).Warn("got nil value")
		return kv.ErrMissingValue
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			log.WithError(err).Error("could not get key for predicate")
			return err
		}

		if valuePredicate != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				log.WithField("predicate", nil).Trace("predicate condition failed")
				return kv.ErrPredicateFailed
			}
			if valuePredicate != kv.PrecondConditionalExists {
				val, err := item.ValueCopy(nil)
				if err != nil {
					log.WithError(err).Error("could not get byte value for predicate")
					return err
				}
				if !bytes.Equal(val, valuePredicate.([]byte)) {
					log.WithField("predicate", valuePredicate).WithField("value", val).Trace("predicate condition failed")
					return kv.ErrPredicateFailed
				}
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			log.WithField("predicate", valuePredicate).Trace("predicate condition failed (key not found)")
			return kv.ErrPredicateFailed
		}

		return txn.Set(k, value)
	})
	if errors.Is(err, badger.ErrConflict) { // Return predicate failed on transaction conflict - to retry
		log.WithError(err).Trace("transaction conflict")
		err = kv.ErrPredicateFailed
	}
	took := time.Since(start)
	log.WithField("took", took).Trace("operation complete")

	return err
}

func deleteFromTxn(ctx context.Context, log logging.Logger, txn *badger.Txn, partitionKey, key []byte) error {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log = log.WithFields(logging.Fields{
		"key": string(k),
		"op":  "delete",
	}).WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return kv.ErrMissingKey
	}
	err := txn.Delete(k)
	took := time.Since(start)
	log = log.WithField("took", took)
	if err != nil {
		log.WithError(err).Trace("operation failed")
		return err
	}
	log.Trace("operation complete")
	return nil
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return deleteFromTxn(ctx, s.logger, txn, partitionKey, key)
	})
}

func scanFromTxn(ctx context.Context, log logging.Logger, txn *badger.Txn, partitionKey []byte, prefetchSize int, options kv.ScanOptions) (kv.EntriesIterator, error) {
	log = log.WithFields(logging.Fields{
		"partition_key": string(partitionKey),
		"start_key":     string(options.KeyStart),
		"op":            "scan",
	}).WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return nil, kv.ErrMissingPartitionKey
	}

	prefix := partitionRange(partitionKey)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = prefetchSize
	if options.BatchSize != 0 && opts.PrefetchSize != 0 && options.BatchSize < opts.PrefetchSize {
		opts.PrefetchSize = options.BatchSize
	}
	if opts.PrefetchSize > 0 {
		opts.PrefetchValues = true
	}
	opts.Prefix = prefix
	iter := txn.NewIterator(opts)
	return &EntriesIterator{
		iter:         iter,
		partitionKey: partitionKey,
		start:        composeKey(partitionKey, options.KeyStart),
		logger:       log,
		txn:          txn,
	}, nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	txn := s.db.NewTransaction(false)
	return scanFromTxn(ctx, s.logger, txn, partitionKey, s.prefetchSize, options)
}

func (s *Store) Close() {
	driverLock.Lock()
	defer driverLock.Unlock()
	s.refCount--
	if s.refCount <= 0 {
		_ = s.db.Close()
		delete(dbMap, s.path)
	}
}

func (s *Store) Transact(ctx context.Context, fn func(operations kv.Operations) error, opts kv.TransactionOpts) error {
	for {
		err := s.db.Update(func(txn *badger.Txn) error {
			return fn(s.newBadgerOperations(txn))
		})
		if err == nil || !errors.Is(err, badger.ErrConflict) {
			// TODO(ariels): Wrap err in a kv-ish error.
			return err
		}
		if opts.Backoff != nil {
			duration := opts.Backoff.NextBackOff()
			if duration == backoff.Stop {
				break
			}
			time.Sleep(duration)
		}
	}
	return kv.ErrConflict
}

func (s *Store) newBadgerOperations(txn *badger.Txn) *operations {
	return &operations{
		s.logger.WithField("txn", time.Now().String()),
		txn,
		s.prefetchSize,
	}
}

type operations struct {
	logger       logging.Logger
	txn          *badger.Txn
	prefetchSize int
}

func (op *operations) Get(ctx context.Context, partitionKey, key []byte) ([]byte, error) {
	return getFromTxn(ctx, op.logger, op.txn, partitionKey, key)
}

func (op *operations) Set(ctx context.Context, partitionKey, key, value []byte) error {
	return setFromTxn(ctx, op.logger, op.txn, partitionKey, key, value)
}

func (op *operations) Delete(ctx context.Context, partitionKey, key []byte) error {
	return deleteFromTxn(ctx, op.logger, op.txn, partitionKey, key)
}

func (op *operations) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	return scanFromTxn(ctx, op.logger, op.txn, partitionKey, op.prefetchSize, options)
}
