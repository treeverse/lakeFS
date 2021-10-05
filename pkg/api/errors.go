package api

import (
	"errors"
)

var (
	ErrFailedToAccessStorage = errors.New("failed to access storage")

	ErrUnexpectedSigningMethod = errors.New("unexpected signing method")

	ErrAuthenticatingRequest = errors.New("error authenticating request")

	ErrInvalidAPIEndpoint = errors.New("invalid API endpoint")
)
