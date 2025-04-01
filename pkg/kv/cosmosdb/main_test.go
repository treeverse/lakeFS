package cosmosdb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/cosmosdb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	testParams *kvparams.CosmosDB
	client     *azcosmos.Client
	throughput = azcosmos.NewManualThroughputProperties(400)
)

func TestCosmosDB(t *testing.T) {
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()

		databaseClient, err := client.NewDatabase(testParams.Database)
		if err != nil {
			t.Fatalf("creating database client: %s", err)
		}

		testParams.Container = "test-container" + testutil.UniqueName()
		log.Printf("Creating container %s", testParams.Container)
		resp2, err := databaseClient.CreateContainer(ctx, azcosmos.ContainerProperties{
			ID: testParams.Container,
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths: []string{"/partitionKey"},
			},
		}, &azcosmos.CreateContainerOptions{ThroughputProperties: &throughput})
		if err != nil {
			t.Fatalf("creating container: %v, raw response: %v", err, resp2.RawResponse)
		}

		store, err := kv.Open(ctx, kvparams.Config{CosmosDB: testParams, Type: cosmosdb.DriverName})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", cosmosdb.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}

type TestingTransport struct{}

func (t *TestingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	originalResp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Check if the original request was successful and content type is JSON
	if originalResp.StatusCode != http.StatusOK {
		return originalResp, nil // Return original non-OK response as is
	}

	// Basic check for JSON content type (can be more robust)
	contentType := originalResp.Header.Get("Content-Type")
	if contentType != "application/json" {
		return originalResp, nil // Return original non-JSON response as is
	}

	if req.Method != http.MethodGet || req.URL.Path != "" {
		return originalResp, nil // Return original response as is
	}

	// parse account properties response
	originalBodyBytes, err := io.ReadAll(originalResp.Body)
	if err != nil {
		// Cannot modify if we can't read it. Return an error or the original response?
		// Returning an error might be cleaner here.
		return nil, fmt.Errorf("failed to read original body: %w", err)
	}
	_ = originalResp.Body.Close()

	// Parse the original JSON body into a map, delete specified keys, and marshal it back
	var dataMap map[string]any
	if err := json.Unmarshal(originalBodyBytes, &dataMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal original JSON: %w", err)
	}

	// Dont't want the azcosmos client use the values from the server
	delete(dataMap, "readableLocations")
	delete(dataMap, "writableLocations")

	modifiedBodyBytes, err := json.Marshal(dataMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal modified JSON: %w", err)
	}
	originalResp.Body = streaming.NopCloser(bytes.NewReader(modifiedBodyBytes)) // Reset the body
	return originalResp, nil
}

func TestMain(m *testing.M) {
	// This part hangs for macOS users, and fails. skipping - see Issue#8476 for more details
	//if runtime.GOOS == "darwin" {
	//	return
	//}
	// use defer to ensure cleanup is called even if os.Exit is called
	var code int
	defer func() {
		os.Exit(code)
	}()

	databaseURI, cleanupFunc, err := testutil.GetCosmosDBInstance()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	defer cleanupFunc()

	const clientTimeout = 30 * time.Second
	testParams = &kvparams.CosmosDB{
		Endpoint: databaseURI,
		Key:      "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
		Database: "test-db",
		Client: &http.Client{
			Timeout:   clientTimeout,
			Transport: &TestingTransport{},
		},
		StrongConsistency: true,
	}

	cred, err := azcosmos.NewKeyCredential(testParams.Key)
	if err != nil {
		log.Fatalf("creating credential key: %v", err)
	}

	// Create a CosmosDB client
	client, err = azcosmos.NewClientWithKey(testParams.Endpoint, cred, &azcosmos.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			InsecureAllowCredentialWithHTTP: true,
			Transport:                       testParams.Client,
		},
	})
	if err != nil {
		log.Fatalf("creating client using access key: %v", err)
	}

	log.Printf("creating database %s", testParams.Database)
	ctx := context.Background()
	_, err = client.CreateDatabase(ctx,
		azcosmos.DatabaseProperties{ID: testParams.Database},
		&azcosmos.CreateDatabaseOptions{ThroughputProperties: &throughput},
	)
	if err != nil {
		log.Fatalf("creating database failed: %v", err)
	}

	code = m.Run()
}
