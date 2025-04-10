package api

import (
	"errors"
)

var (
	ErrFailedToAccessStorage = errors.New("failed to access storage")
	ErrAuthenticatingRequest = errors.New("error authenticating request")
	ErrInvalidAPIEndpoint    = errors.New("invalid API endpoint")
	ErrRequestSizeExceeded   = errors.New("request size exceeded")
	ErrStorageNamespaceInUse = errors.New("storage namespace already in use")
)
