package local

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

func partitionRange(partitionKey []byte) []byte {
	return append(partitionKey, kv.PathDelimiter[0])
}

func composeKey(partitionKey, key []byte) []byte {
	return append(partitionRange(partitionKey), key...)
}

type Store struct {
	db           *badger.DB
	logger       logging.Logger
	prefetchSize int
	refCount     int
	path         string
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log := s.logger.WithField("key", string(k)).WithField("op", "get").WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return nil, kv.ErrMissingKey
	}

	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(composeKey(partitionKey, key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return kv.ErrNotFound
		}
		if err != nil {
			log.WithError(err).Error("error getting key")
			return err
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			log.WithError(err).Error("error getting value for key")
			return err
		}
		return nil
	})
	log.WithField("took", time.Since(start)).WithError(err).WithField("size", len(value)).Trace("operation complete")
	if err != nil {
		return nil, err
	}

	return &kv.ValueWithPredicate{
		Value:     value,
		Predicate: kv.Predicate(value),
	}, nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log := s.logger.WithField("key", string(k)).WithField("op", "set").WithContext(ctx)
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
		return txn.Set(k, value)
	})
	if err != nil {
		log.WithError(err).Error("error setting value")
		return err
	}
	log.WithField("took", time.Since(start)).Trace("done setting value")
	return nil
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
			val, err := item.ValueCopy(nil)
			if err != nil {
				log.WithError(err).Error("could not get byte value for predicate")
				return err
			}
			if !bytes.Equal(val, valuePredicate.([]byte)) {
				log.WithField("predicate", valuePredicate).WithField("value", val).Trace("predicate condition failed")
				return kv.ErrPredicateFailed
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			log.WithField("predicate", valuePredicate).Trace("predicate condition failed (key not found)")
			return kv.ErrPredicateFailed
		}

		return txn.Set(composeKey(partitionKey, key), value)
	})
	if errors.Is(err, badger.ErrConflict) { // Return predicate failed on transaction conflict - to retry
		log.WithError(err).Trace("transaction conflict")
		err = kv.ErrPredicateFailed
	}
	took := time.Since(start)
	log.WithField("took", took).Trace("operation complete")

	return err
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	k := composeKey(partitionKey, key)
	start := time.Now()
	log := s.logger.
		WithField("key", string(k)).
		WithField("op", "delete").
		WithContext(ctx)
	log.Trace("performing operation")
	if len(partitionKey) == 0 {
		log.WithError(kv.ErrMissingPartitionKey).Warn("got empty partition key")
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		log.WithError(kv.ErrMissingKey).Warn("got empty key")
		return kv.ErrMissingKey
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
	took := time.Since(start)
	log = log.WithField("took", took)
	if err != nil {
		log.WithError(err).Trace("operation failed")
		return err
	}
	log.Trace("operation complete")
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	log := s.logger.WithFields(logging.Fields{
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
	txn := s.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = s.prefetchSize
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

func (s *Store) Close() {
	driverLock.Lock()
	defer driverLock.Unlock()
	s.refCount--
	if s.refCount <= 0 {
		_ = s.db.Close()
		delete(dbMap, s.path)
	}
}
