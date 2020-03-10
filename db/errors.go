package db

import "golang.org/x/xerrors"

var (
	ErrNotFound      = xerrors.New("not found")
	ErrSerialization = xerrors.New("serialization error")
)
