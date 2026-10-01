package actions_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLuaRun_SDKAllowedHosts checks that the SDK-backed Lua libraries send their requests through
// the allowed hosts check: a loopback server is refused by default.  gcloud builds its own
// authentication, so it is also checked to reach the token_uri once allowed.
func TestLuaRun_SDKAllowedHosts(t *testing.T) {
	gcloudScript := func(t *testing.T, u string) string {
		return fmt.Sprintf(`local c = require("gcloud").gs_client(%q)
c.write_fuse_symlink("from/path", "gs://bucket/object", {})`, serviceAccountJSON(t, u+"/token"))
	}
	tests := []struct {
		name    string
		allowed bool
		script  func(t *testing.T, serverURL string) string
	}{
		{
			name: "aws_s3_endpoint",
			script: func(_ *testing.T, u string) string {
				return fmt.Sprintf(`local c = require("aws").s3_client("ak", "sk", "us-east-1", %q)
c.get_object("bucket", "key")`, u)
			},
		},
		{
			name: "aws_glue_endpoint",
			script: func(_ *testing.T, u string) string {
				return fmt.Sprintf(`local c = require("aws").glue_client("ak", "sk", "us-east-1", %q)
c.get_table("db", "table")`, u)
			},
		},
		{
			name: "databricks_host",
			script: func(_ *testing.T, u string) string {
				return fmt.Sprintf(`local c = require("databricks").client(%q, "token")
c.create_schema("schema", "catalog", false)`, u)
			},
		},
		{
			// the storage account name is placed in the host part of the endpoint URL
			name: "azure_storage_account",
			script: func(_ *testing.T, u string) string {
				return fmt.Sprintf(`local c = require("azure").blob_client(%q, "a2V5")
c.get_object("container/key")`, strings.TrimPrefix(u, "http://")+"/probe?")
			},
		},
		{name: "gcloud_token_uri", script: gcloudScript},
		{name: "gcloud_token_uri_allowed", allowed: true, script: gcloudScript},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel() // SDK retries make blocked cases take seconds
			hits := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits++
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()

			var allowedHosts []string
			if tt.allowed {
				allowedHosts = []string{"127.0.0.1"}
			}
			h := newLuaActionHookWithAllowedHosts(t, nil, "", false, allowedHosts, tt.script(t, server.URL))
			_, err := runHook(t.Context(), h)
			if err == nil {
				t.Fatal("expected the request to the test server to fail the hook")
			}
			if blocked := strings.Contains(err.Error(), "destination host not allowed"); blocked == tt.allowed {
				t.Fatalf("blocked=%t, expected %t: %s", blocked, !tt.allowed, err)
			}
			if (hits > 0) != tt.allowed {
				t.Fatalf("got %d hits with allowed=%t: %s", hits, tt.allowed, err)
			}
		})
	}
}

// serviceAccountJSON returns service account credentials whose tokens are requested from tokenURI.
func serviceAccountJSON(t *testing.T, tokenURI string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: must(x509.MarshalPKCS8PrivateKey(key))})
	data, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "project",
		"private_key_id": "key-id",
		"private_key":    string(keyPEM),
		"client_email":   "hook@project.iam.gserviceaccount.com",
		"token_uri":      tokenURI,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
