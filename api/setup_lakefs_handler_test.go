package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/testutil"
)

func Test_setupLakeFSHandler(t *testing.T) {
	// get handler with DB without apply the DDL
	handler, deps := getHandler(t, testutil.WithGetDBApplyDDL(false))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	user := model.User{
		CreatedAt:   time.Now(),
		DisplayName: "admin",
	}
	req, err := json.Marshal(user)
	if err != nil {
		t.Fatal("JSON marshal request", err)
	}

	reqURI := srv.URL + api.SetupLakeFSRoute
	const contentType = "application/json"
	t.Run("fresh start", func(t *testing.T) {
		// request to setup
		res, err := http.Post(reqURI, contentType, bytes.NewReader(req))
		if err != nil {
			t.Fatal("Post setup request to server", err)
		}
		defer func() {
			_ = res.Body.Close()
		}()

		const expectedStatusCode = http.StatusOK
		if res.StatusCode != expectedStatusCode {
			t.Fatalf("setup request returned %d status, expected %d", res.StatusCode, expectedStatusCode)
		}

		// read response
		var credKeys model.CredentialKeys
		err = json.NewDecoder(res.Body).Decode(&credKeys)
		if err != nil {
			t.Fatal("Decode response", err)
		}

		if len(credKeys.AccessKeyId) == 0 {
			t.Fatal("Credential key id is missing")
		}

		foundCreds, err := deps.auth.GetCredentials(credKeys.AccessKeyId)
		if err != nil {
			t.Fatal("Get API credentials key id for created access key", err)
		}
		if foundCreds == nil {
			t.Fatal("Get API credentials secret key for created access key")
		}
		if foundCreds.AccessSecretKey != credKeys.AccessSecretKey {
			t.Fatalf("Access secret key '%s', expected '%s'", foundCreds.AccessSecretKey, credKeys.AccessSecretKey)
		}
	})

	// now we ask again - should get status conflict
	t.Run("existing setup", func(t *testing.T) {
		// request to setup
		res, err := http.Post(reqURI, contentType, bytes.NewReader(req))
		if err != nil {
			t.Fatal("Post setup request to server", err)
		}
		defer res.Body.Close()

		const expectedStatusCode = http.StatusConflict
		if res.StatusCode != expectedStatusCode {
			t.Fatalf("setup request returned %d status, expected %d", res.StatusCode, expectedStatusCode)
		}
	})
}
