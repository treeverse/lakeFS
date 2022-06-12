package api

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"

	"github.com/gorilla/sessions"
	"golang.org/x/oauth2"
)

const (
	OIDCAuthSessionName = "auth_session"

	IdTokenClaimsSessionKey = "id_token_claims"
	StateSessionKey         = "state"
)

// NewOIDCLoginPageHandler returns a handler to redirect the user the OIDC provider's login page.
func NewOIDCLoginPageHandler(sessionStore sessions.Store, oauthConfig *oauth2.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state, err := generateRandomState()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		session, err := sessionStore.Get(r, OIDCAuthSessionName)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		session.Values[StateSessionKey] = state
		if err := session.Save(r, w); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
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
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	state := base64.StdEncoding.EncodeToString(b)
	return state, nil
}
