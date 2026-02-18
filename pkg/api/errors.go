package api

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/auth"
)

var (
	ErrFailedToAccessStorage = errors.New("failed to access storage")
	ErrAuthenticatingRequest = auth.ErrAuthenticatingRequest
	ErrInvalidAPIEndpoint    = errors.New("invalid API endpoint")
	ErrRequestSizeExceeded   = errors.New("request size exceeded")
	ErrStorageNamespaceInUse = errors.New("storage namespace already in use")
	ErrInvalidFormat         = errors.New("invalid format")
)
