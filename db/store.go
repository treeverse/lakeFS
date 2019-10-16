package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type FDBStore struct {
	db     fdb.Database
	spaces map[string]subspace.Subspace
}

func NewFDBStore(db fdb.Database, spaces map[string]subspace.Subspace) *FDBStore {
	return &FDBStore{
		db:     db,
		spaces: spaces,
	}
}

func (s *FDBStore) Space(name string) subspace.Subspace {
	return s.spaces[name]
}

func (s *FDBStore) ReadTransact(ctx []tuple.TupleElement, fn func(q ReadQuery) (interface{}, error)) (interface{}, error) {
	return s.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		q := &FDBReadQuery{
			Context: ctx,
			tx:      tx,
		}
		return fn(q)
	})
}

func (s *FDBStore) Transact(ctx []tuple.TupleElement, fn func(q Query) (interface{}, error)) (interface{}, error) {
	return s.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		q := &FDBQuery{
			FDBReadQuery: &FDBReadQuery{
				Context: ctx,
				tx:      tx,
			},
			tx: tx,
		}
		return fn(q)
	})
}
