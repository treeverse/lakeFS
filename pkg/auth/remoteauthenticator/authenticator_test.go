package remoteauthenticator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/remoteauthenticator"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

// mockAuthService is a minimal mock implementation of auth.Service for testing
type mockAuthService struct {
	auth.Service
	user *model.User
}

func (m *mockAuthService) GetUser(_ context.Context, _ string) (*model.User, error) {
	if m.user != nil {
		return m.user, nil
	}
	return nil, auth.ErrNotFound
}

func (m *mockAuthService) CreateUser(_ context.Context, user *model.User) (string, error) {
	m.user = user
	return user.Username, nil
}

func (m *mockAuthService) AddUserToGroup(_ context.Context, _, _ string) error {
	return nil
}

func TestAuthenticator_RequestIDPropagation(t *testing.T) {
	const requestID = "test-request-id-remote-auth"
	called := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if actual := r.Header.Get("X-Request-ID"); actual != requestID {
			t.Errorf("Got request ID %q, expected %q", actual, requestID)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	mockService := &mockAuthService{}
	authenticator, err := remoteauthenticator.NewAuthenticator(
		remoteauthenticator.AuthenticatorConfig{
			Enabled:          true,
			Endpoint:         server.URL,
			DefaultUserGroup: "TestGroup",
			RequestTimeout:   10 * time.Second,
		},
		mockService,
		logging.ContextUnavailable(),
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), httputil.RequestIDContextKey, requestID)
	_, err = authenticator.AuthenticateUser(ctx, "testuser", "testpass")
	require.NoError(t, err)
	require.True(t, called, "Expected server to be called")
}
