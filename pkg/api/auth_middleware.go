package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	//"gopkg.in/dgrijalva/jwt-go.v3"
)

var (
	//ErrUnexpectedSigningMethod = errors.New("unexpected signing method")
	ErrAuthenticationFailed = errors.New("error authenticating request")
)

/*
// NewBasicAuthHandler returns a function that hooks into Swagger's basic Auth provider
// it uses the Auth.Service provided to ensure credentials are valid
func NewBasicAuthHandler(authService auth.Service) func(accessKey, secretKey string) (user *models.User, err error) {
	logger := logging.Default().WithField("auth", "basic")
	return func(accessKey, secretKey string) (user *models.User, err error) {
		// Unfortunately, go-swagger doesn't expose the request or the request context to authentication handlers
		ctx := context.Background()
		credentials, err := authService.GetCredentials(ctx, accessKey)
		if err != nil {
			logger.WithError(err).WithField("access_key", accessKey).Debug("could not get access key for login")
			return nil, ErrAuthenticationFailed
		}
		if secretKey != credentials.AccessSecretKey {
			logger.WithField("access_key", accessKey).Debug("access key secret does not match")
			return nil, ErrAuthenticationFailed
		}
		userData, err := authService.GetUserByID(ctx, credentials.UserID)
		if err != nil {
			logger.WithField("access_key", accessKey).Debug("could not find user for key pair")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{
			ID: userData.Username,
		}, nil
	}
}

// NewJwtTokenAuthHandler decodes, validates and authenticates a user that exists
// in the X-JWT-Authorization header.
// This header either exists natively, or is set using a token
func NewJwtTokenAuthHandler(authService auth.Service) func(string) (*models.User, error) {
	logger := logging.Default().WithField("auth", "jwt")
	return func(tokenString string) (*models.User, error) {
		// Unfortunately, go-swagger doesn't expose the request or the request context to authentication handlers
		ctx := context.Background()

		claims := &jwt.StandardClaims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Header["alg"])
			}
			return authService.SecretStore().SharedSecret(), nil
		})
		if err != nil {
			return nil, ErrAuthenticationFailed
		}
		claims, ok := token.Claims.(*jwt.StandardClaims)
		if !ok || !token.Valid {
			return nil, ErrAuthenticationFailed
		}
		cred, err := authService.GetCredentials(ctx, claims.Subject)
		if err != nil {
			logger.WithField("subject", claims.Subject).Debug("could not find credentials for token")
			return nil, ErrAuthenticationFailed
		}
		userData, err := authService.GetUserByID(ctx, cred.UserID)
		if err != nil {
			logger.WithFields(logging.Fields{
				"user_id": cred.UserID,
				"subject": claims.Subject,
			}).Debug("could not find user id by credentials")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{
			ID: userData.Username,
		}, nil
	}
}
*/

func AuthMiddleware(logger logging.Logger, authService auth.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			var user *model.User
			// validate jwt token

			// validate using basic auth
			user, err := authenticateBasicAuth(ctx, logger, authService, r)
			if err != nil {
				logger.WithError(err).Debug("authenticate with basic auth")
			}
			if user != nil {
				r = r.WithContext(context.WithValue(r.Context(), "user", user))
			}
			next.ServeHTTP(w, r)
		})
	}
}

func authenticateBasicAuth(ctx context.Context, logger logging.Logger, authService auth.Service, r *http.Request) (*model.User, error) {
	accessKey, secretKey, ok := r.BasicAuth()
	if !ok {
		return nil, nil
	}
	credentials, err := authService.GetCredentials(ctx, accessKey)
	if err != nil {
		return nil, fmt.Errorf("basic auth could not get access key: %w", err)
	}
	if secretKey != credentials.AccessSecretKey {
		logger.WithField("access_key", accessKey).Debug("access key secret does not match")
		return nil, ErrAuthenticationFailed
	}
	user, err := authService.GetUserByID(ctx, credentials.UserID)
	if err != nil {
		return nil, fmt.Errorf("basic auth cloud not find user for key: %w", err)
	}
	return user, nil
}
