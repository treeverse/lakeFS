package api

import (
	"errors"
)

var (
	// ErrEnsureStorageNamespace is an error returned when trying to create a new repository
	// and the attempt to access the storage fails
	ErrEnsureStorageNamespace = errors.New("failed to ensure access to the storage")

	ErrUnexpectedSigningMethod = errors.New("unexpected signing method")

	ErrAuthenticationFailed = errors.New("error authenticating request")

	ErrInvalidAPIEndpoint = errors.New("invalid API endpoint")
)
