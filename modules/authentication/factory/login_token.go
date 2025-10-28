package factory

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type UnimplementedLoginTokenProvider struct{}

func (l UnimplementedLoginTokenProvider) GetRedirect(ctx context.Context) (*authentication.TokenRedirect, error) {
	return nil, authentication.ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) Release(ctx context.Context, mailbox string) error {
	return authentication.ErrNotImplemented
}

func (l UnimplementedLoginTokenProvider) GetToken(ctx context.Context, mailbox string) (string, time.Time, error) {
	return "", time.Time{}, authentication.ErrNotImplemented
}

func NewLoginTokenProvider(_ context.Context, c config.Config, logger logging.Logger, store kv.Store) (authentication.LoginTokenProvider, error) {
	return UnimplementedLoginTokenProvider{}, nil
}
