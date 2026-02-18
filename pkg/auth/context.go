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

// WithoutUser removes any authenticated user from the context.
// Unlike WithUser(ctx, nil) which stores a typed nil that passes type assertions,
// this stores untyped nil so that GetUser will return ErrUserNotFound.
func WithoutUser(ctx context.Context) context.Context {
	return context.WithValue(ctx, userContextKey, nil) //nolint:staticcheck
}

func CopyUserFromContext(srcCtx, dstCtx context.Context) context.Context {
	if user, _ := GetUser(srcCtx); user != nil {
		return WithUser(dstCtx, user)
	}
	return dstCtx
}
