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
		logger.WithError(err).Error("failed to verify bearer token")
		return nil, ErrAuthenticatingRequest
	}

	username := claims.Subject
	userData, err := authService.GetUser(ctx, username)
	if err != nil {
		logger.WithError(err).WithFields(logging.Fields{
			"token_id": claims.ID,
			"username": username,
			"subject":  username,
		}).Debug("could not find user id by credentials")
		return nil, fmt.Errorf("get user: %w", err)
	}
	return userData, nil
}

func UserByAuth(ctx context.Context, logger logging.Logger, authenticator Authenticator, authService Service, accessKey string, secretKey string) (*model.User, error) {
	username, err := authenticator.AuthenticateUser(ctx, accessKey, secretKey)
	if err != nil {
		logger.WithError(err).WithField("accessKey", accessKey).Error("authenticate")
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidSecretAccessKey) {
			return nil, fmt.Errorf("%w: %w", ErrAuthenticatingRequest, err)
		}
		return nil, err
	}
	user, err := authService.GetUser(ctx, username)
	if err != nil {
		logger.WithError(err).WithFields(logging.Fields{"username": username}).Debug("could not find user id by credentials")
		return nil, err
	}
	return user, nil
}
