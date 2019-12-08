package db

import (
	"github.com/dgraph-io/badger"
)

type DBStore struct {
	db *badger.DB
}

func NewDBStore(db *badger.DB) *DBStore {
	return &DBStore{db: db}
}

func (s *DBStore) ReadTransact(fn func(q ReadQuery) (interface{}, error)) (interface{}, error) {
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

func (s *DBStore) Transact(fn func(q Query) (interface{}, error)) (interface{}, error) {
	//return s.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
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
