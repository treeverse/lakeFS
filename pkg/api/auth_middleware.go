package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/go-openapi/swag"
	"github.com/golang-jwt/jwt"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/oidc"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
)

// extractSecurityRequirements using Swagger returns an array of security requirements set for the request.
func extractSecurityRequirements(router routers.Router, r *http.Request) (openapi3.SecurityRequirements, error) {
	// Find route
	route, _, err := router.FindRoute(r)
	if err != nil {
		return nil, err
	}
	if route.Operation.Security == nil {
		return route.Swagger.Security, nil
	}
	return *route.Operation.Security, nil
}

func AuthMiddleware(logger logging.Logger, swagger *openapi3.Swagger, authenticator auth.Authenticator, authService auth.Service, sessionStore sessions.Store, oidcConfig *config.OIDC) func(next http.Handler) http.Handler {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			securityRequirements, err := extractSecurityRequirements(router, r)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			user, err := checkSecurityRequirements(r, securityRequirements, logger, authenticator, authService, sessionStore, oidcConfig)
			if err != nil {
				writeError(w, http.StatusUnauthorized, err)
				return
			}
			if user != nil {
				r = r.WithContext(context.WithValue(r.Context(), UserContextKey, user))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// checkSecurityRequirements goes over the security requirements and check the authentication. returns the user information and error if the security check was required.
// it will return nil user and error in case of no security checks to match.
func checkSecurityRequirements(r *http.Request,
	securityRequirements openapi3.SecurityRequirements,
	logger logging.Logger,
	authenticator auth.Authenticator,
	authService auth.Service,
	sessionStore sessions.Store,
	oidcConfig *config.OIDC,
) (*model.User, error) {
	ctx := r.Context()
	var user *model.User
	var err error

	logger = logger.WithContext(ctx)
	for _, securityRequirement := range securityRequirements {
		for provider := range securityRequirement {
			switch provider {
			case "jwt_token":
				// validate jwt token from header
				authHeaderValue := r.Header.Get("Authorization")
				if authHeaderValue == "" {
					continue
				}
				parts := strings.Fields(authHeaderValue)
				if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
					continue
				}
				token := parts[1]
				user, err = userByToken(ctx, logger, authService, token)
			case "basic_auth":
				// validate using basic auth
				accessKey, secretKey, ok := r.BasicAuth()
				if !ok {
					continue
				}
				user, err = userByAuth(ctx, logger, authenticator, authService, accessKey, secretKey)
			case "cookie_auth":
				var internalAuthSession *sessions.Session
				internalAuthSession, _ = sessionStore.Get(r, InternalAuthSessionName)
				token := ""
				if internalAuthSession != nil {
					token, _ = internalAuthSession.Values[TokenSessionKeyName].(string)
				}
				if token == "" {
					continue
				}
				user, err = userByToken(ctx, logger, authService, token)
			case "oidc_auth":
				var oidcSession *sessions.Session
				oidcSession, err = sessionStore.Get(r, OIDCAuthSessionName)
				if err != nil {
					return nil, err
				}
				user, err = userFromOIDC(ctx, logger, authService, oidcSession, oidcConfig)
			default:
				// unknown security requirement to check
				logger.WithField("provider", provider).Error("Authentication middleware unknown security requirement provider")
				return nil, ErrAuthenticatingRequest
			}
			if err != nil {
				return nil, err
			}
			if user != nil {
				return user, nil
			}
		}
	}
	return nil, nil
}

func enhanceWithFriendlyName(user *model.User, friendlyName string) *model.User {
	if friendlyName != "" {
		user.FriendlyName = swag.String(friendlyName)
	}
	return user
}

// userFromOIDC returns a user from an existing OIDC session.
// If the user doesn't exist on the lakeFS side, it is created.
// This function does not make any calls to an external provider.
func userFromOIDC(ctx context.Context, logger logging.Logger, authService auth.Service, authSession *sessions.Session, oidcConfig *config.OIDC) (*model.User, error) {
	idTokenClaims, ok := authSession.Values[IDTokenClaimsSessionKey].(oidc.Claims)
	if !ok || idTokenClaims == nil {
		return nil, ErrAuthenticatingRequest
	}
	externalID, ok := idTokenClaims["sub"].(string)
	if !ok {
		logger.WithField("sub", idTokenClaims["sub"]).Error("Failed type assertion for sub claim")
		return nil, ErrAuthenticatingRequest
	}

	for claimName, expectedValue := range oidcConfig.ValidateIDTokenClaims {
		actualValue, ok := idTokenClaims[claimName]
		if !ok || actualValue != expectedValue {
			logger.WithFields(logging.Fields{
				"claim_name":     claimName,
				"actual_value":   actualValue,
				"expected_value": expectedValue,
				"missing":        !ok,
			}).Error("Authentication failed on validating ID token claims")
			return nil, ErrAuthenticatingRequest
		}
	}

	friendlyName := ""
	if oidcConfig.FriendlyNameClaimName != "" {
		friendlyName, _ = idTokenClaims[oidcConfig.FriendlyNameClaimName].(string)
	}
	user, err := authService.GetUserByExternalID(ctx, externalID)
	if err == nil {
		return enhanceWithFriendlyName(user, friendlyName), nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		logger.WithError(err).Error("Failed to get external user from database")
		return nil, ErrAuthenticatingRequest
	}
	u := model.User{
		CreatedAt:  time.Now().UTC(),
		Source:     "oidc",
		Username:   externalID,
		ExternalID: &externalID,
	}
	_, err = authService.CreateUser(ctx, &u)
	if err != nil {
		if !errors.Is(err, db.ErrAlreadyExists) {
			logger.WithError(err).Error("Failed to create external user in database")
			return nil, ErrAuthenticatingRequest
		}
		// user already exists - get it:
		user, err = authService.GetUserByExternalID(ctx, externalID)
		if err != nil {
			logger.WithError(err).Error("Failed to get external user from database")
			return nil, ErrAuthenticatingRequest
		}
		return enhanceWithFriendlyName(user, friendlyName), nil
	}
	initialGroups := oidcConfig.DefaultInitialGroups
	if userInitialGroups, ok := idTokenClaims[oidcConfig.InitialGroupsClaimName].(string); ok {
		initialGroups = strings.Split(userInitialGroups, ",")
	}
	for _, g := range initialGroups {
		err = authService.AddUserToGroup(ctx, u.Username, strings.TrimSpace(g))
		if err != nil {
			logger.WithError(err).Error("Failed to add external user to group")
		}
	}
	return enhanceWithFriendlyName(&u, friendlyName), nil
}

func userByToken(ctx context.Context, logger logging.Logger, authService auth.Service, tokenString string) (*model.User, error) {
	claims, err := auth.VerifyToken(authService.SecretStore().SharedSecret(), tokenString)
	// make sure no audience is set for login token
	if err != nil || !claims.VerifyAudience(LoginAudience, false) {
		return nil, ErrAuthenticatingRequest
	}

	username := claims.Subject
	userData, err := authService.GetUser(ctx, username)
	if err != nil {
		logger.WithFields(logging.Fields{
			"token_id": claims.Id,
			"username": username,
			"subject":  claims.Subject,
		}).Debug("could not find user id by credentials")
		return nil, ErrAuthenticatingRequest
	}
	return userData, nil
}

func userByAuth(ctx context.Context, logger logging.Logger, authenticator auth.Authenticator, authService auth.Service, accessKey string, secretKey string) (*model.User, error) {
	// TODO(ariels): Rename keys.
	username, err := authenticator.AuthenticateUser(ctx, accessKey, secretKey)
	if err != nil {
		logger.WithError(err).WithField("user", accessKey).Error("authenticate")
		return nil, ErrAuthenticatingRequest
	}
	user, err := authService.GetUser(ctx, username)
	if err != nil {
		logger.WithError(err).WithFields(logging.Fields{"user_name": username}).Debug("could not find user id by credentials")
		return nil, ErrAuthenticatingRequest
	}
	return user, nil
}

func VerifyResetPasswordToken(ctx context.Context, authService auth.Service, token string) (*jwt.StandardClaims, error) {
	secret := authService.SecretStore().SharedSecret()
	claims, err := auth.VerifyTokenWithAudience(secret, token, auth.ResetPasswordAudience)
	if err != nil {
		return nil, err
	}
	tokenID := claims.Id
	tokenExpiresAt := claims.ExpiresAt
	err = authService.ClaimTokenIDOnce(ctx, tokenID, tokenExpiresAt)
	if err != nil {
		return nil, err
	}
	return claims, nil
}
