package gateway

import "golang.org/x/xerrors"

var (
	ErrAuthenticationMissing = xerrors.New("no valid authentication information provided")
)
