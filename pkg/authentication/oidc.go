package authentication

import (
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"net/http"
)

type OIDCRoutesRegisterer interface {
	RegisterOIDCRoutes(r *chi.Mux, sessionStore sessions.Store)
}

type OIDCCallbackHandler interface {
	OIDCCallback(w http.ResponseWriter, r *http.Request, sessionStore sessions.Store)
}

type OIDCProvider interface {
	OIDCRoutesRegisterer
	OIDCCallbackHandler
}
