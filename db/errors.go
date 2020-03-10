package db

import "golang.org/x/xerrors"

var (
	ErrNotFound       = xerrors.New("not found")
	ErrBranchNotFound = xerrors.Errorf("branch : %w", ErrNotFound)
	ErrSerialization  = xerrors.New("serialization error")
)
