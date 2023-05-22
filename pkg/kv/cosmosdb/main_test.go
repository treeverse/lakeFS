package cosmosdb_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var testParams *kvparams.CosmosDB

func TestMain(m *testing.M) {
	databaseURI, cleanupFunc, err := testutil.GetCosmosDBInstance()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	testParams = &kvparams.CosmosDB{
		Endpoint:     databaseURI,
		ReadWriteKey: "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
		Database:     "test-db",
		Container:    "test-container",
	}

	cred, err := azcosmos.NewKeyCredential(testParams.ReadWriteKey)
	if err != nil {
		log.Fatalf("creating key: %v", err)
	}
	// Create a CosmosDB client
	client, err := azcosmos.NewClientWithKey(testParams.Endpoint, cred, nil)
	if err != nil {
		log.Fatalf("creating client using access key: %v", err)
	}
	ctx := context.Background()
	resp, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: testParams.Database}, nil)
	if err != nil {
		print(resp.RawResponse)
		log.Fatalf("creating database: %v", err)
	}

	databaseClient, err := client.NewDatabase(testParams.Database)
	if err != nil {
		log.Fatalf("creating database client: %v", err)
	}
	resp2, err := databaseClient.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: testParams.Container,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/id"},
		},
	}, nil)
	if err != nil {
		print(resp2.RawResponse)
		log.Fatalf("creating container: %v", err)
	}

	code := m.Run()
	cleanupFunc()
	os.Exit(code)
}
