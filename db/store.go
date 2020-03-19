package db

import (
	"github.com/dgraph-io/badger"
	"github.com/treeverse/lakefs/logging"
)

// logging adapter
type badgerLogger struct {
	logger logging.Logger
}

func (l *badgerLogger) Errorf(s string, d ...interface{}) {
	l.logger.Errorf(s, d...)
}
func (l *badgerLogger) Warningf(s string, d ...interface{}) {
	l.logger.Warningf(s, d...)
}
func (l *badgerLogger) Infof(s string, d ...interface{}) {
	l.logger.Infof(s, d...)
}
func (l *badgerLogger) Debugf(s string, d ...interface{}) {
	l.logger.Debugf(s, d...)
}

func NewBadgerLoggingAdapter(logger logging.Logger) badger.Logger {
	return &badgerLogger{logger: logger}
}

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
