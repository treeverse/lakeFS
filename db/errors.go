package db

import (
	"errors"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrSerialization = errors.New("serialization error")
)
