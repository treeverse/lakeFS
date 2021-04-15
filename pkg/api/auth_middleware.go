package api

import (
	"context"
	"crypto/subtle"
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
			var err error

			// validate jwt token from cookie
			jwtCookie, _ := r.Cookie(JWTCookieName)
			if jwtCookie != nil {
				user, err = userByToken(ctx, logger, authService, jwtCookie.Value)
				if err != nil {
					writeError(w, http.StatusUnauthorized, err)
					return
				}
			}

			// validate jwt token from header
			if user == nil {
				token := r.Header.Get(JWTAuthorizationHeaderName)
				if token != "" {
					user, err = userByToken(ctx, logger, authService, token)
					if err != nil {
						writeError(w, http.StatusUnauthorized, err)
						return
					}
				}
			}

			// validate using basic auth
			if user == nil {
				accessKey, secretKey, ok := r.BasicAuth()
				if ok {
					user, err = userByAuth(ctx, logger, authService, accessKey, secretKey)
					if err != nil {
						writeError(w, http.StatusUnauthorized, err)
						return
					}
				}
			}

			// keep user on the request context
			if user != nil {
				r = r.WithContext(context.WithValue(r.Context(), UserContextKey, user))
			}
			next.ServeHTTP(w, r)
		})
	}
}

func userByToken(ctx context.Context, logger logging.Logger, authService auth.Service, tokenString string) (*model.User, error) {
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
		logger.WithField("subject", claims.Subject).Info("could not find credentials for token")
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
	return userData, nil
}

func userByAuth(ctx context.Context, logger logging.Logger, authService auth.Service, accessKey string, secretKey string) (*model.User, error) {
	cred, err := authService.GetCredentials(ctx, accessKey)
	if err != nil {
		logger.WithError(err).Error("failed getting credentials for key")
		return nil, ErrAuthenticationFailed
	}
	if subtle.ConstantTimeCompare([]byte(secretKey), []byte(cred.SecretAccessKey)) != 1 {
		logger.Debug("access key secret does not match")
		return nil, ErrAuthenticationFailed
	}
	user, err := authService.GetUserByID(ctx, cred.UserID)
	if err != nil {
		logger.WithFields(logging.Fields{"user_id": cred.UserID}).Debug("could not find user id by credentials")
		return nil, ErrAuthenticationFailed
	}
	return user, nil
}
