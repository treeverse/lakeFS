package api

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/treeverse/lakefs/auth/model"

	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/auth"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	User            = "user"
	AccessKeyId     = "access_key_id"
	AccessSecretKey = "access_secret_key"
)

var (
	ErrAuthenticationError = xerrors.New("authentication failed")
	ErrAuthorizationError  = xerrors.New("you are not permitted to perform this action")
)

// implementation of credentials.PerRPCCredentials used to pass
type accessKeyCredentials struct {
	accessKey    string
	accessSecret string
}

func (c *accessKeyCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		AccessKeyId:     c.accessKey,
		AccessSecretKey: c.accessSecret,
	}, nil
}

func (c *accessKeyCredentials) RequireTransportSecurity() bool {
	return false
}

func AccessKeyCredentials(accessKeyId, accessSecretKey string) credentials.PerRPCCredentials {
	return &accessKeyCredentials{accessKeyId, accessSecretKey}
}

// server side authenticator
type keyAuthenticator struct {
	authService auth.Service
}

type Authenticator interface {
	Authenticate(ctx context.Context) (context.Context, error)
}

func KeyAuthenticator(authService auth.Service) Authenticator {
	return &keyAuthenticator{authService}
}

func (a *keyAuthenticator) Authenticate(ctx context.Context) (context.Context, error) {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, ErrAuthenticationError
	}
	keyMatches := meta.Get(AccessKeyId)
	secretMatches := meta.Get(AccessSecretKey)
	if len(keyMatches) != 1 || len(secretMatches) != 1 {
		return ctx, ErrAuthenticationError
	}
	key := keyMatches[0]
	secret := secretMatches[0]

	credentials, err := a.authService.GetAPICredentials(key)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, ErrAuthenticationError.Error())
	}
	if !strings.EqualFold(credentials.GetAccessSecretKey(), secret) {
		return ctx, status.Error(codes.Unauthenticated, ErrAuthenticationError.Error())
	}
	user, err := a.authService.GetUser(credentials.GetEntityId())
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, ErrAuthenticationError.Error())
	}

	// write user to context
	ctx = context.WithValue(ctx, User, user)
	return ctx, nil
}

func getUser(ctx context.Context) *model.User {
	return ctx.Value(User).(*model.User)
}
