package auth

import "golang.org/x/xerrors"

var (
	ErrInvalidArn              = xerrors.New("invalid ARN")
	ErrInsufficientPermissions = xerrors.New("insufficient permissions")
)
