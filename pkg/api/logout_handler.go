package api

import (
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	ReturnToQueryParamName = "returnTo"
)

// NewLogoutHandler returns a handler to clear the user sessions and redirect the user to the login page.
func NewLogoutHandler(sessionStore sessions.Store, logger logging.Logger, logoutRedirectURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := clearSession(w, r, sessionStore, InternalAuthSessionName)
		if err != nil {
			logger.WithError(err).Error("Failed to clear internal session during logout")
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		err = clearSession(w, r, sessionStore, OIDCAuthSessionName)
		if err != nil {
			logger.WithError(err).Error("Failed to clear OIDC session during logout")
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		redirectURL := r.URL.Query().Get(ReturnToQueryParamName)
		if redirectURL == "" {
			redirectURL = logoutRedirectURL
		}
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	}
}
