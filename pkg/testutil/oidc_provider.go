package testutil

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
)

// MockOidcProvider is a nop implementation of the OidcProvider interface.
// Use for tests that don't need to test the OIDC provider.
type MockOidcProvider struct {
}

func (m *MockOidcProvider) RegisterOIDCRoutes(_ *chi.Mux, _ sessions.Store) {
	// nop
}

func (m *MockOidcProvider) OIDCCallback(_ http.ResponseWriter, _ *http.Request, _ sessions.Store) {
	// nop
}
