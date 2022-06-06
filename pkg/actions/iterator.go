package actions

import (
	"errors"
)

var ErrIteratorClosed = errors.New("iterator closed")

type RunResultIterator interface {
	Next() bool
	Value() *RunResult
	Err() error
	Close()
}

type TaskResultIterator interface {
	Next() bool
	Value() *TaskResult
	Err() error
	Close()
}
