package authentication

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/authentication/apiclient"
	"github.com/treeverse/lakefs/pkg/authentication/jwtidp"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// JWTService decorates an inner Service with JWT IdP login support.
// All methods other than ValidateJWT delegate to the inner Service.
// Instantiate via WithJWT.
type JWTService struct {
	inner       Service
	verifier    *jwtidp.Verifier
	resolver    *jwtidp.Config
	authService auth.Service
	logger      logging.Logger
}

// WithJWT wraps inner with JWT IdP login support. It returns inner
// unchanged when cfg is nil or cfg.JWKSURL is empty. Fails fast if
// the JWKS endpoint is unreachable or the resolver config is invalid,
// so misconfiguration surfaces at startup rather than on first login.
func WithJWT(inner Service, cfg *config.JWT, authService auth.Service, logger logging.Logger) (Service, error) {
	if cfg == nil || cfg.JWKSURL == "" {
		return inner, nil
	}
	verifier, err := jwtidp.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("jwt verifier: %w", err)
	}
	resolver, err := jwtidp.NewResolverConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("jwt resolver: %w", err)
	}
	return &JWTService{
		inner:       inner,
		verifier:    verifier,
		resolver:    resolver,
		authService: authService,
		logger:      logger,
	}, nil
}

func (s *JWTService) ValidateJWT(ctx context.Context, token string) (*model.User, error) {
	claims, err := s.verifier.Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	return jwtidp.ResolveUser(ctx, s.logger, s.authService, s.resolver, claims)
}

func (s *JWTService) IsExternalPrincipalsEnabled() bool {
	return s.inner.IsExternalPrincipalsEnabled()
}

func (s *JWTService) ExternalPrincipalLogin(ctx context.Context, identityRequest map[string]any) (*apiclient.ExternalPrincipal, error) {
	return s.inner.ExternalPrincipalLogin(ctx, identityRequest)
}

func (s *JWTService) ValidateSTS(ctx context.Context, code, redirectURI, state string) (string, error) {
	return s.inner.ValidateSTS(ctx, code, redirectURI, state)
}

func (s *JWTService) RegisterAdditionalRoutes(r *chi.Mux, sessionStore sessions.Store) {
	s.inner.RegisterAdditionalRoutes(r, sessionStore)
}

func (s *JWTService) OauthCallback(w http.ResponseWriter, r *http.Request, sessionStore sessions.Store) {
	s.inner.OauthCallback(w, r, sessionStore)
}
