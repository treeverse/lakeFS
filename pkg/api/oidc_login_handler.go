package api

import (
	"net/http"

	"github.com/gorilla/sessions"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/logging"
	"golang.org/x/oauth2"
)

const (
	IDTokenClaimsSessionKey = "id_token_claims"
	StateSessionKey         = "state"

	stateLength = 22
)

// NewOIDCLoginPageHandler returns a handler to redirect the user the OIDC provider's login page.
func NewOIDCLoginPageHandler(sessionStore sessions.Store, oauthConfig *oauth2.Config, logger logging.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// clear internal authentication session
		err := clearSession(w, r, sessionStore, InternalAuthSessionName)
		if err != nil {
			logger.WithError(err).Error("Failed to clear internal auth session")
		}
		state, err := nanoid.New(stateLength)
		if err != nil {
			logger.WithError(err).Error("failed to generate state for oidc")
			writeError(w, http.StatusInternalServerError, "Failed to redirect to login page")
			return
		}

		session, _ := sessionStore.Get(r, OIDCAuthSessionName)
		session.Values[StateSessionKey] = state
		if err := session.Save(r, w); err != nil {
			logger.WithError(err).Error("failed to save oidc session")
			writeError(w, http.StatusInternalServerError, "Failed to save auth session")
			return
		}
		http.Redirect(w, r, oauthConfig.AuthCodeURL(state), http.StatusTemporaryRedirect)
	}
}
