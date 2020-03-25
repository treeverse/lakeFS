package db

import (
	"github.com/dgraph-io/badger"
)

type LocalDBStore struct {
	db *badger.DB
}

func NewLocalDBStore(db *badger.DB) *LocalDBStore {
	return &LocalDBStore{db: db}
}

func (s *LocalDBStore) ReadTransact(fn func(q ReadQuery) (interface{}, error)) (interface{}, error) {
	var val interface{}
	err := s.db.View(func(tx *badger.Txn) error {
		q := &DBReadQuery{tx: tx}
		v, err := fn(q)
		val = v
		return err

	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *LocalDBStore) Transact(fn func(q Query) (interface{}, error)) (interface{}, error) {
	var val interface{}
	err := s.db.Update(func(tx *badger.Txn) error {
		q := &DBQuery{
			&DBReadQuery{tx: tx},
		}
		v, err := fn(q)
		val = v
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}
