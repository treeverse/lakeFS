package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
)

func TestAuthMiddleware(t *testing.T) {
	handler, deps := setupHandler(t, "mem")
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
		apiToken := testGenerateApiToken(t, deps.authService, cred.AccessKeyID)
		authProvider, err := securityprovider.NewSecurityProviderApiKey("header", api.JWTAuthorizationHeaderName, apiToken)
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
		apiToken := testGenerateApiToken(t, deps.authService, "AKIAIOSFODNN7EXAMPLE")
		authProvider, err := securityprovider.NewSecurityProviderApiKey("header", api.JWTAuthorizationHeaderName, apiToken)
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

	t.Run("valid jwt cookie", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateApiToken(t, deps.authService, cred.AccessKeyID)
		authProvider, err := securityprovider.NewSecurityProviderApiKey("cookie", api.JWTCookieName, apiToken)
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

	t.Run("invalid jwt cookie", func(t *testing.T) {
		ctx := context.Background()
		apiToken := testGenerateApiToken(t, deps.authService, "AKIAIOSFODNN7EXAMPLE")
		authProvider, err := securityprovider.NewSecurityProviderApiKey("cookie", api.JWTCookieName, apiToken)
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
}

func testGenerateApiToken(t testing.TB, authService auth.Service, accessKeyID string) string {
	t.Helper()
	secret := authService.SecretStore().SharedSecret()
	now := time.Now()
	expires := now.Add(time.Hour)
	tokenString, err := api.GenerateJWT(secret, accessKeyID, now, expires)
	if err != nil {
		t.Fatal("Failed to generate jwt token:", err)
	}
	return tokenString
}
