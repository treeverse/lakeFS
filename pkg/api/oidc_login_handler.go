package api

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"

	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/logging"
	"golang.org/x/oauth2"
)

const (
	OIDCAuthSessionName = "auth_session"

	IDTokenClaimsSessionKey = "id_token_claims"
	StateSessionKey         = "state"

	stateByteSize = 32
)

// NewOIDCLoginPageHandler returns a handler to redirect the user the OIDC provider's login page.
func NewOIDCLoginPageHandler(sessionStore sessions.Store, oauthConfig *oauth2.Config, logger logging.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, err := generateRandomState()
		if err != nil {
			logger.Errorf("failed to generate state for oidc: %w", err)
			writeError(w, http.StatusInternalServerError, "Failed to redirect to login page")
			return
		}

		session, err := sessionStore.Get(r, OIDCAuthSessionName)
		if err != nil {
			logger.Errorf("failed to get oidc session: %w", err)
			writeError(w, http.StatusInternalServerError, "Failed to redirect to login page")
			return
		}
		session.Values[StateSessionKey] = state
		if err := session.Save(r, w); err != nil {
			logger.Errorf("failed to save oidc session: %w", err)
			writeError(w, http.StatusInternalServerError, "Failed to redirect to login page")
			return
		}
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		u := url.URL{
			Scheme: scheme,
			Host:   r.Host,
			Path:   BaseURL + "/oidc/callback",
		}
		oauthConfig.RedirectURL = u.String()
		http.Redirect(w, r, oauthConfig.AuthCodeURL(state), http.StatusTemporaryRedirect)
	}
}

func generateRandomState() (string, error) {
	b := make([]byte, stateByteSize)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	state := base64.StdEncoding.EncodeToString(b)
	return state, nil
}
