package authentication

import (
	"context"
	"time"
)

type TokenRedirect struct {
	RedirectURL string
	Mailbox     string
}

type LoginTokenProvider interface {
	// GetRedirect is called to start logging in via an authenticated user.  It is called
	// unauthenticated, with no user on the context.
	GetRedirect(ctx context.Context) (*TokenRedirect, error)
	// Release drops token into mailbox, releasing it for the next GetToken call.  It is
	// called authenticated.
	Release(ctx context.Context, mailbox string) error
	// GetToken returns a token waiting on mailbox.  It is called unauthenticated.
	GetToken(ctx context.Context, mailbox string) (string, time.Time, error)
}
