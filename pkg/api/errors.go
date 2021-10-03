package api

import (
	"errors"
)

var (
	// ErrEnsureStorageNamespace is an error returned when trying to create a new repository
	// and the try to access the storage fails
	ErrEnsureStorageNamespace = errors.New("failed to ensure access to the storage")
)
