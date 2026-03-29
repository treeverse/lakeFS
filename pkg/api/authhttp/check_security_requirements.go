package authhttp

import (
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

// CheckSecurityRequirements goes over the security requirements and checks the authentication.
// It returns the user information and error if the security check was required.
// It will return nil user and nil error in case of no security checks to match.
func CheckSecurityRequirements(r *http.Request,
	securityRequirements openapi3.SecurityRequirements,
	logger logging.Logger,
	authenticator auth.Authenticator,
	authService auth.Service,
	sessionStore sessions.Store,
	oidcConfig *auth.OIDCConfig,
	cookieAuthConfig *auth.CookieAuthConfig,
) (*model.User, error) {
	ctx := r.Context()
	logger = logger.WithContext(ctx)

	for _, securityRequirement := range securityRequirements {
		for provider := range securityRequirement {
			var (
				user *model.User
				err  error
			)

			switch provider {
			case "jwt_token":
				authHeaderValue := r.Header.Get("Authorization")
				if authHeaderValue == "" {
					continue
				}
				parts := strings.Fields(authHeaderValue)
				if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
					continue
				}
				user, err = auth.UserByToken(ctx, authService, parts[1])
			case "basic_auth":
				accessKey, secretKey, ok := r.BasicAuth()
				if !ok {
					continue
				}
				user, err = auth.UserByAuth(ctx, authenticator, authService, accessKey, secretKey)
			case "cookie_auth":
				internalAuthSession, _ := sessionStore.Get(r, auth.InternalAuthSessionName)
				token := ""
				if internalAuthSession != nil {
					token, _ = internalAuthSession.Values[auth.TokenSessionKeyName].(string)
				}
				if token == "" {
					continue
				}
				user, err = auth.UserByToken(ctx, authService, token)
			case "oidc_auth":
				oidcSession, getErr := sessionStore.Get(r, auth.OIDCAuthSessionName)
				if getErr != nil {
					return nil, getErr
				}
				user, err = auth.UserFromOIDCSession(ctx, logger, authService, oidcSession, oidcConfig)
			case "saml_auth":
				samlSession, getErr := sessionStore.Get(r, auth.SAMLAuthSessionName)
				if getErr != nil {
					return nil, getErr
				}
				user, err = auth.UserFromSAMLSession(ctx, logger, authService, samlSession, cookieAuthConfig)
			default:
				logger.WithField("provider", provider).Error("Authentication middleware unknown security requirement provider")
				return nil, auth.ErrAuthenticatingRequest
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
