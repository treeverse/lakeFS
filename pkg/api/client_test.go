package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	genclient "github.com/treeverse/lakefs/pkg/api/gen/client"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestNewClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		expectedPath := genclient.DefaultBasePath + "/repositories"
		if req.URL.Path != expectedPath {
			t.Fatalf("Client request path %s, expected %s", req.URL.Path, expectedPath)
		}
		_, _ = rw.Write([]byte(`OK`))
	}))
	// Close the server when test finishes
	defer server.Close()

	client, err := NewClient(server.URL, "key", "secret")
	testutil.Must(t, err)
	_, _, _ = client.ListRepositories(context.Background(), "", 0)

	client, err = NewClient(server.URL+genclient.DefaultBasePath, "key2", "secret2")
	testutil.Must(t, err)
	_, _, _ = client.ListRepositories(context.Background(), "", 0)
}
