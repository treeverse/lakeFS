package api_test

import (
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/testutil"

	"github.com/go-openapi/runtime/client"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
)

func TestBasicAuth(t *testing.T) {
	clt, _ := setupClient(t, "")
	creds := createDefaultAdminUser(t, clt)

	t.Run("valid Auth", func(t *testing.T) {
		validAuth := client.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)
		_, err := clt.Repositories.ListRepositories(repositories.NewListRepositoriesParamsWithTimeout(timeout), validAuth)
		if err != nil {
			t.Fatalf("unexpected error \"%s\" when passing valid credentials", err)
		}
	})

	t.Run("invalid Auth secret", func(t *testing.T) {
		invalidAuth := client.BasicAuth(creds.AccessKeyID, "foobarbaz")
		_, err := clt.Repositories.ListRepositories(repositories.NewListRepositoriesParams().WithTimeout(timeout), invalidAuth)
		var unauthorized *repositories.ListRepositoriesUnauthorized
		if !errors.As(err, &unauthorized) {
			t.Fatalf("got %s not unauthorized error", err)
		}
	})
}

func TestJwtTokenAuth(t *testing.T) {
	clt, deps := setupClient(t, "")
	creds := createDefaultAdminUser(t, clt)
	secret := deps.authService.SecretStore().SharedSecret()

	t.Run("valid token", func(t *testing.T) {
		now := time.Now()
		exp := now.Add(10 * time.Minute)
		token, err := api.GenerateJWT(secret, creds.AccessKeyID, now, exp)
		testutil.MustDo(t, "create auth token", err)
		authInfo := client.APIKeyAuth(api.JWTAuthorizationHeaderName, "header", token)
		_, err = clt.Repositories.ListRepositories(repositories.NewListRepositoriesParamsWithTimeout(timeout), authInfo)
		testutil.MustDo(t, "Request expected to work using a valid token", err)
	})

	t.Run("invalid token", func(t *testing.T) {
		now := time.Now()
		exp := now.Add(10 * time.Minute)
		token, err := api.GenerateJWT(secret, "admin", now, exp)
		testutil.MustDo(t, "create auth token", err)
		authInfo := client.APIKeyAuth(api.JWTAuthorizationHeaderName, "header", token)
		_, err = clt.Repositories.ListRepositories(repositories.NewListRepositoriesParams().WithTimeout(timeout), authInfo)
		var unauthorized *repositories.ListRepositoriesUnauthorized
		if !errors.As(err, &unauthorized) {
			t.Fatalf("got %s not unauthorized error", err)
		}
	})
}
