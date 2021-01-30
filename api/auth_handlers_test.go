package api_test

import (
	"errors"
	"testing"

	"github.com/go-openapi/runtime/client"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
)

func TestServer_BasicAuth(t *testing.T) {
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
