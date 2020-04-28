package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func Test_setupHandler(t *testing.T) {
	// get handler with DB without apply the DDL
	handler, _ := getHandler(t, testutil.WithGetDBApplyDDL(false))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := json.Marshal(struct {
		Email    string `json:"email"`
		FullName string `json:"full_name"`
	}{
		Email:    "tester@treeverse.io",
		FullName: "Test Name",
	})
	if err != nil {
		t.Fatal("JSON marshal request", err)
	}

	reqURI := srv.URL + "/setup"
	const contentType = "application/json"
	t.Run("fresh start", func(t *testing.T) {
		// request to setup
		res, err := http.Post(reqURI, contentType, bytes.NewReader(req))
		if err != nil {
			t.Fatal("Post setup request to server", err)
		}
		defer res.Body.Close()

		const expectedStatusCode = http.StatusOK
		if res.StatusCode != expectedStatusCode {
			t.Fatalf("setup request returned %d status, expected %d", res.StatusCode, expectedStatusCode)
		}

		// read response
		cred := struct {
			AccessKeyID     string `json:"access_key_id"`
			SecretAccessKey string `json:"secret_access_key"`
		}{}
		err = json.NewDecoder(res.Body).Decode(&cred)
		if err != nil {
			t.Fatal("Decode response", err)
		}

		// TODO(barak): do we have a better way to validate?

		const expectedKeyLen = 20
		if len(cred.AccessKeyID) != expectedKeyLen {
			t.Fatalf("Key >%s< len %d, expected %d", cred.AccessKeyID, len(cred.AccessKeyID), expectedKeyLen)
		}
		const expectedSecretLen = 40
		if len(cred.SecretAccessKey) != expectedSecretLen {
			t.Fatalf("Secret >%s< len %d, expected %d", cred.SecretAccessKey, len(cred.SecretAccessKey), expectedSecretLen)
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
