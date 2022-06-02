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
)

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
		session.Values["state"] = state
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

func NewOIDCLogoutHandler(sessionStore sessions.Store, oauthConfig *oauth2.Config, logoutUrl string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		session, err := sessionStore.Get(r, OIDCAuthSessionName)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		//idTokenHint, _ := session.Values["id_token"].(string)

		session.Values = map[interface{}]interface{}{}
		if err := session.Save(r, w); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}

		returnTo, err := url.Parse(scheme + "://" + r.Host)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		parameters := url.Values{}
		//parameters.Add("id_token_hint", idTokenHint)
		parameters.Add("post_logout_redirect_uri", returnTo.String())
		u, err := url.Parse(logoutUrl)
		if err != nil {
			// TODO log error
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		}
		u.RawQuery = parameters.Encode()
		http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
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
