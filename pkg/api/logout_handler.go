package api

import (
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/logging"
)

// NewLogoutHandler returns a handler to clear the user sessions and redirect the user to the login page.
func NewLogoutHandler(sessionStore sessions.Store, logger logging.Logger, logoutRedirectURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := clearSession(w, r, sessionStore, InternalAuthSessionName)
		if err != nil {
			logger.WithError(err).Error("Failed to clear internal session during logout")
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		err = clearSession(w, r, sessionStore, OIDCAuthSessionName)
		if err != nil {
			logger.WithError(err).Error("Failed to clear OIDC session during logout")
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		http.Redirect(w, r, logoutRedirectURL, http.StatusTemporaryRedirect)
	}
}

func clearSession(w http.ResponseWriter, r *http.Request, sessionStore sessions.Store, sessionName string) error {
	session, _ := sessionStore.Get(r, sessionName)
	session.Options.MaxAge = -1
	return session.Save(r, w)
}
