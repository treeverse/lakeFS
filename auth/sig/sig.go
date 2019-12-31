package sig

import "golang.org/x/xerrors"

var (
	ErrHeaderMalformed        = xerrors.New("header malformed")
	ErrMissingDateHeader      = xerrors.New("missing X-Amz-Date or Date header")
	ErrDateHeaderMalformed    = xerrors.New("wrong format for date header")
	ErrSignatureDateMalformed = xerrors.New("signature date malformed")
	ErrBadSignature           = xerrors.New("bad signature")
	ErrMissingAuthData        = xerrors.New("missing authorization information")
)

type SigContext interface {
	GetAccessKeyId() string
}

type Credentials interface {
	GetAccessSecretKey() string
}

type SigAuthenticator interface {
	Parse() (SigContext, error)
	Verify(Credentials) error
}
