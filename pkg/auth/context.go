package auth

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth/model"
)

type contextKey string

const (
	userContextKey contextKey = "user"
)

func GetUser(ctx context.Context) (*model.User, error) {
	user, ok := ctx.Value(userContextKey).(*model.User)
	if !ok {
		return nil, ErrUserNotFound
	}
	return user, nil
}

func WithUser(ctx context.Context, user *model.User) context.Context {
	return context.WithValue(ctx, userContextKey, user)
}
