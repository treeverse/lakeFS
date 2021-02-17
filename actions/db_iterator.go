package actions

import (
	"errors"

	sq "github.com/Masterminds/squirrel"
)

type iteratorState int

const (
	iteratorStateInit iteratorState = iota
	iteratorStateQuery
	iteratorStateDone
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

var (
	ErrIteratorClosed   = errors.New("iterator closed")
	ErrSeekNotSupported = errors.New("seek not supported")
)
