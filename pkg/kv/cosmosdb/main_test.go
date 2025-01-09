package cosmosdb_test

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

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
		if runtime.GOOS == "darwin" {
			t.Skipf("this test hangs for macOS users, and fails. skipping - see Issue#8476 for more details")
		}

		t.Helper()

		databaseClient, err := client.NewDatabase(testParams.Database)
		if err != nil {
			log.Fatalf("creating database client: %v", err)
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
			print(resp2.RawResponse)
			t.Fatalf("creating container: %v", err)
		}

		store, err := kv.Open(ctx, kvparams.Config{CosmosDB: testParams, Type: cosmosdb.DriverName})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", cosmosdb.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}

func TestMain(m *testing.M) {
	if runtime.GOOS == "darwin" {
		// this part hangs for macOS users, and fails. skipping - see Issue#8476 for more details
		return
	}

	databaseURI, cleanupFunc, err := testutil.GetCosmosDBInstance()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	defer cleanupFunc()

	testParams = &kvparams.CosmosDB{
		Endpoint: databaseURI,
		Key:      "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
		Database: "test-db",
		Client: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // ignore self-signed cert for local testing using the emulator
		}},
		StrongConsistency: false,
	}

	cred, err := azcosmos.NewKeyCredential(testParams.Key)
	if err != nil {
		log.Fatalf("creating key: %v", err)
	}
	// Create a CosmosDB client
	client, err = azcosmos.NewClientWithKey(testParams.Endpoint, cred, &azcosmos.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: &http.Client{Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}},
		},
	})
	if err != nil {
		log.Fatalf("creating client using access key: %v", err)
	}

	log.Printf("Creating database %s", testParams.Database)
	ctx := context.Background()
	resp, err := client.CreateDatabase(ctx,
		azcosmos.DatabaseProperties{ID: testParams.Database},
		&azcosmos.CreateDatabaseOptions{ThroughputProperties: &throughput},
	)
	if err != nil {
		print(resp.RawResponse)
		log.Fatalf("creating database: %v", err)
	}

	code := m.Run()
	os.Exit(code)
}
