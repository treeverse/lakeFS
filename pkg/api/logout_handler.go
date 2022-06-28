package api

import (
	"net/http"
	"strconv"

	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/logging"
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
		redirectToOIDC, _ := strconv.ParseBool(r.URL.Query().Get("redirectToOIDC"))
		if redirectToOIDC {
			http.Redirect(w, r, "/oidc/login", http.StatusTemporaryRedirect)
			return
		}
		http.Redirect(w, r, logoutRedirectURL, http.StatusTemporaryRedirect)
	}
}
