package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"gopkg.in/dgrijalva/jwt-go.v3"
)

const JWTAuthorizationHeaderName = "X-JWT-Authorization"

var (
	ErrUnexpectedSigningMethod = errors.New("unexpected signing method")
	ErrAuthenticationFailed    = errors.New("error authenticating request")
)

func AuthMiddleware(logger logging.Logger, authService auth.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			var user *model.User

			// validate jwt token from cookie
			jwtCookie, _ := r.Cookie(JWTCookieName)
			if jwtCookie != nil {
				user = userByToken(ctx, w, logger, authService, jwtCookie.Value)
				if user == nil {
					return
				}
			}

			// validate jwt token from header
			if user == nil {
				token := r.Header.Get(JWTAuthorizationHeaderName)
				if token != "" {
					user = userByToken(ctx, w, logger, authService, token)
					if user == nil {
						return
					}
				}
			}

			// validate using basic auth
			if user == nil {
				accessKey, secretKey, ok := r.BasicAuth()
				if ok {
					user = userByAuth(ctx, w, logger, authService, accessKey, secretKey)
					if user == nil {
						return
					}
				}
			}

			// keep user on the request context
			r = r.WithContext(context.WithValue(r.Context(), UserContextKey, user))
			next.ServeHTTP(w, r)
		})
	}
}

func userByToken(ctx context.Context, w http.ResponseWriter, logger logging.Logger, authService auth.Service, tokenString string) *model.User {
	claims := &jwt.StandardClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Header["alg"])
		}
		return authService.SecretStore().SharedSecret(), nil
	})
	if err != nil {
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	claims, ok := token.Claims.(*jwt.StandardClaims)
	if !ok || !token.Valid {
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	cred, err := authService.GetCredentials(ctx, claims.Subject)
	if err != nil {
		logger.WithField("subject", claims.Subject).Debug("could not find credentials for token")
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	userData, err := authService.GetUserByID(ctx, cred.UserID)
	if err != nil {
		logger.WithFields(logging.Fields{
			"user_id": cred.UserID,
			"subject": claims.Subject,
		}).Debug("could not find user id by credentials")
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	return userData
}

func userByAuth(ctx context.Context, w http.ResponseWriter, logger logging.Logger, authService auth.Service, accessKey string, secretKey string) *model.User {
	cred, err := authService.GetCredentials(ctx, accessKey)
	if err != nil {
		logger.WithError(err).Error("failed to get credentials for key")
		writeError(w, http.StatusInternalServerError, fmt.Errorf("get credentials for access key: %w", err))
		return nil
	}
	if secretKey != cred.AccessSecretKey {
		logger.Debug("access key secret does not match")
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	user, err := authService.GetUserByID(ctx, cred.UserID)
	if err != nil {
		logger.WithFields(logging.Fields{
			"user_id": cred.UserID,
		}).Debug("could not find user id by credentials")
		writeError(w, http.StatusUnauthorized, ErrAuthenticationFailed)
		return nil
	}
	return user
}
