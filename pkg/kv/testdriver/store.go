package testdriver

import (
	"context"

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type StoreCallback int

const (
	GetCallback StoreCallback = iota
	SetCallback
	SetIfCallback
	DeleteCallback
	ScanCallback
)

type GetCB func(partitionKey, key []byte) (*kv.ValueWithPredicate, error)
type SetCB func(partitionKey, key []byte) error
type SetIfCB func(partitionKey, key []byte) error
type DeleteCB func(partitionKey, key []byte) error
type ScanCB func(partitionKey, start []byte) (kv.EntriesIterator, error)

type Store struct {
	store kv.Store
	cbMap map[StoreCallback]interface{}
	log   logging.Logger
}

type TestStore interface {
	kv.Store
	SetStoreCallback(cb StoreCallback, fn interface{})
}

func NewTestStore(store kv.Store) TestStore {
	log := logging.Default().WithField("test", "TestStore")
	return &Store{
		store: store,
		cbMap: map[StoreCallback]interface{}{},
		log:   log,
	}
}

func (s Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if cb, ok := s.cbMap[GetCallback]; ok {
		if f, k := cb.(GetCB); k {
			s.log.Info("Running test driver callback for 'Get' operation")
			return f(partitionKey, key)
		} else {
			panic("invalid callback")
		}
	}
	return s.store.Get(ctx, partitionKey, key)
}

func (s Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	if cb, ok := s.cbMap[SetCallback]; ok {
		if f, k := cb.(SetCB); k {
			s.log.Info("Running test driver callback for 'Set' operation")
			return f(partitionKey, key)
		} else {
			panic("invalid callback")
		}
	}
	return s.store.Set(ctx, partitionKey, key, value)
}

func (s Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	if cb, ok := s.cbMap[SetIfCallback]; ok {
		s.log.Info("Running test driver callback for 'SetIf' operation")
		if f, k := cb.(SetIfCB); k {
			return f(partitionKey, key)
		} else {
			panic("invalid callback")
		}
	}
	return s.store.SetIf(ctx, partitionKey, key, value, valuePredicate)
}

func (s Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if cb, ok := s.cbMap[DeleteCallback]; ok {
		if f, k := cb.(DeleteCB); k {
			s.log.Info("Running test driver callback for 'Delete' operation")
			return f(partitionKey, key)
		} else {
			panic("invalid callback")
		}
	}
	return s.store.Delete(ctx, partitionKey, key)
}

func (s Store) Scan(ctx context.Context, partitionKey, start []byte) (kv.EntriesIterator, error) {
	if cb, ok := s.cbMap[ScanCallback]; ok {
		if f, k := cb.(ScanCB); k {
			s.log.Info("Running test driver callback for 'Scan' operation")
			return f(partitionKey, start)
		} else {
			panic("invalid callback")
		}
	}
	return s.store.Scan(ctx, partitionKey, start)
}

func (s Store) Close() {
	s.store.Close()
}

func (s Store) SetStoreCallback(cb StoreCallback, fn interface{}) {
	s.cbMap[cb] = fn
}
