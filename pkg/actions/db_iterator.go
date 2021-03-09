package actions

import (
	"errors"

	sq "github.com/Masterminds/squirrel"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

var (
	ErrIteratorClosed = errors.New("iterator closed")
)
