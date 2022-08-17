package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

func TestDBAuthMiddleware(t *testing.T) {
	testAuthMiddleware(t, false)
}

func TestKVAuthMiddleware(t *testing.T) {
	testAuthMiddleware(t, true)
}

func testAuthMiddleware(t *testing.T, kvEnabled bool) {
	handler, deps := setupHandler(t, kvEnabled)
	server := setupServer(t, handler)
	apiEndpoint := server.URL + api.BaseURL
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)

	t.Run("valid basic auth", func(t *testing.T) {
		ctx := context.Background()
		authClient := setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("unexpected status code %d, expected %d", resp.StatusCode(), http.StatusOK)
		}
	})

	t.Run("invalid basic auth", func(t *testing.T) {
		ctx := context.Background()
		authClient := setupClientByEndpoint(t, server.URL, "foo", "bar")
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusUnauthorized {
			t.Fatal("ListRepositories() should return unauthorized status code, got", resp.StatusCode())
		}
		if resp.JSON401 == nil {
			t.Fatal("ListRepositories() should return unauthorized response, got nil")
		}
	})

	t.Run("valid jwt header", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateApiToken(ctx, t, clt, cred)
		authProvider, err := securityprovider.NewSecurityProviderApiKey("header", "Authorization", "Bearer "+apiToken)
		if err != nil {
			t.Fatal("basic auth security provider", err)
		}
		authClient, err := api.NewClientWithResponses(apiEndpoint, api.WithRequestEditorFn(authProvider.Intercept))
		if err != nil {
			t.Fatal("failed to create lakefs api client:", err)
		}
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("unexpected status code %d, expected %d", resp.StatusCode(), http.StatusOK)
		}
	})

	t.Run("invalid jwt header", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateBadAPIToken(t, *deps.authService)
		authProvider, err := securityprovider.NewSecurityProviderApiKey("header", "Authorization", "Bearer "+apiToken)
		if err != nil {
			t.Fatal("basic auth security provider", err)
		}
		authClient, err := api.NewClientWithResponses(apiEndpoint, api.WithRequestEditorFn(authProvider.Intercept))
		if err != nil {
			t.Fatal("failed to create lakefs api client:", err)
		}
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusUnauthorized {
			t.Fatal("ListRepositories() should return unauthorized status code, got", resp.StatusCode())
		}
		if resp.JSON401 == nil {
			t.Fatal("ListRepositories() should return unauthorized response, got nil")
		}
	})

	t.Run("valid gorilla session", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateApiToken(ctx, t, clt, cred)
		values := map[interface{}]interface{}{api.TokenSessionKeyName: apiToken}
		store := sessions.NewCookieStore([]byte("some secret"))
		encoded, err := securecookie.EncodeMulti(api.InternalAuthSessionName, values, store.Codecs...)
		if err != nil {
			t.Fatal("Failed to encode cookie value for session: ", err)
		}
		authProvider, err := securityprovider.NewSecurityProviderApiKey("cookie", api.InternalAuthSessionName, encoded)
		if err != nil {
			t.Fatal("gorilla session security provider", err)
		}
		authClient, err := api.NewClientWithResponses(apiEndpoint, api.WithRequestEditorFn(authProvider.Intercept))
		if err != nil {
			t.Fatal("failed to create lakefs api client:", err)
		}
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusOK {
			t.Fatalf("unexpected status code %d, expected %d", resp.StatusCode(), http.StatusOK)
		}
	})

	t.Run("invalid gorilla cookie", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateBadAPIToken(t, *deps.authService)
		values := map[interface{}]interface{}{api.TokenSessionKeyName: apiToken}
		store := sessions.NewCookieStore([]byte("some secret"))
		encoded, err := securecookie.EncodeMulti(api.InternalAuthSessionName, values, store.Codecs...)
		if err != nil {
			t.Fatal("Failed to encode cookie value for session: ", err)
		}
		authProvider, err := securityprovider.NewSecurityProviderApiKey("cookie", api.InternalAuthSessionName, encoded)
		if err != nil {
			t.Fatal("gorilla session security provider", err)
		}
		authClient, err := api.NewClientWithResponses(apiEndpoint, api.WithRequestEditorFn(authProvider.Intercept))
		if err != nil {
			t.Fatal("failed to create lakefs api client:", err)
		}
		resp, err := authClient.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
		if err != nil {
			t.Fatal("ListRepositories() should return without error:", err)
		}
		if resp.StatusCode() != http.StatusUnauthorized {
			t.Fatal("ListRepositories() should return unauthorized status code, got", resp.StatusCode())
		}
		if resp.JSON401 == nil {
			t.Fatal("ListRepositories() should return unauthorized response, got nil")
		}
	})
}

func testGenerateApiToken(ctx context.Context, t testing.TB, clt api.ClientWithResponsesInterface, cred *model.BaseCredential) string {
	t.Helper()
	loginReq := api.LoginJSONRequestBody{
		AccessKeyId:     cred.AccessKeyID,
		SecretAccessKey: cred.SecretAccessKey,
	}
	login, err := clt.LoginWithResponse(ctx, loginReq)
	if err != nil {
		t.Fatal("Login:", err)
	}
	if login.JSON200 == nil {
		t.Fatal("Failed to login:", login.Status())
	}
	return login.JSON200.Token
}

func testGenerateBadAPIToken(t testing.TB, authService auth.Service) string {
	secret := authService.SecretStore().SharedSecret()
	now := time.Now()
	expires := now.Add(time.Hour)
	userID := "2906"
	// Generate a JWT for a nonexistent user.  It will fail authentication.
	tokenString, err := api.GenerateJWTLogin(secret, userID, now, expires)
	if err != nil {
		t.Fatal("Generate (bad) JWT token:", err)
	}
	return tokenString
}
