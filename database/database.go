package database

type TupleElement interface{}

type Tuple []TupleElement

type Value []byte

type Future interface {
	Resolve() (Value, error)
	MustResolve() Value
}

type Transaction interface {
	Set(Tuple, Value) error
	Get(Tuple) Future
	Commit() error
	Rollback() error
}

type Transactor func()

type Database interface {
	Transact(Transactor) error
}
