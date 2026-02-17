package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

func UserByToken(ctx context.Context, logger logging.Logger, authService Service, tokenString string) (*model.User, error) {
	claims, err := VerifyToken(authService.SecretStore().SharedSecret(), tokenString)
	if err != nil {
		return nil, fmt.Errorf("verify token: %w: %w", ErrAuthenticatingRequest, err)
	}

	username := claims.Subject
	userData, err := authService.GetUser(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("get user %s (token %s): %w", username, claims.ID, err)
	}
	return userData, nil
}

func UserByAuth(ctx context.Context, logger logging.Logger, authenticator Authenticator, authService Service, accessKey string, secretKey string) (*model.User, error) {
	username, err := authenticator.AuthenticateUser(ctx, accessKey, secretKey)
	if err != nil {
		// Wrap authentication-specific errors to ensure they return 401 instead of 404/500
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidSecretAccessKey) {
			return nil, fmt.Errorf("%w (access key %s): %w", ErrAuthenticatingRequest, accessKey, err)
		}
		return nil, fmt.Errorf("authenticate access key %s: %w", accessKey, err)
	}
	user, err := authService.GetUser(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("get user %s: %w", username, err)
	}
	return user, nil
}
