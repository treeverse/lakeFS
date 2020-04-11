package store

import (
	"github.com/treeverse/lakefs/db"
)

type Store interface {
	Transact(fn func(ops ClientOperations) (interface{}, error), opts ...db.TxOpt) (interface{}, error)
	RepoTransact(repoId string, fn func(ops RepoOperations) (interface{}, error), opts ...db.TxOpt) (interface{}, error)
}

type DBStore struct {
	db db.Database
}

func NewDBStore(database db.Database) *DBStore {
	return &DBStore{db: database}
}

func (s *DBStore) Transact(fn func(ops ClientOperations) (interface{}, error), opts ...db.TxOpt) (interface{}, error) {
	return s.db.Transact(func(tx db.Tx) (i interface{}, err error) {
		op := &DBClientOperations{tx: tx}
		return fn(op)
	}, opts...)
}

func (s *DBStore) RepoTransact(repoId string, fn func(ops RepoOperations) (interface{}, error), opts ...db.TxOpt) (interface{}, error) {
	return s.db.Transact(func(tx db.Tx) (i interface{}, err error) {
		op := &DBRepoOperations{
			repoId: repoId,
			tx:     tx,
		}
		return fn(op)
	}, opts...)
}
