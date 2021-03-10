package auth

import "errors"

var (
	ErrInvalidArn              = errors.New("invalid ARN")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
)
