package authentication

import (
	"context"
	"time"
)

type UnimplementedLoginTokenProvider struct{}

func (l UnimplementedLoginTokenProvider) GetRedirect(ctx context.Context) (*TokenRedirect, error) {
	return nil, ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) Release(ctx context.Context, loginRequestToken string) error {
	return ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) GetToken(ctx context.Context, mailbox string) (string, time.Time, error) {
	return "", time.Time{}, ErrNotImplemented
}
