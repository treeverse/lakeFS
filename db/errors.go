package db

import (
	"errors"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrSerialization = errors.New("serialization error")
	ErrNotASlice     = errors.New("results must be a pointer to a slice")
)
