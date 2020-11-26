package db

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
)

var (
	ErrNotFound      = fmt.Errorf("not found: %w", pgx.ErrNoRows)
	ErrAlreadyExists = errors.New("already exists")
	ErrSerialization = errors.New("serialization error")
	ErrNotASlice     = errors.New("results must be a pointer to a slice")
)
