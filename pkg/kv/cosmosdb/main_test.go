package cosmosdb_test

import (
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"os"
	"testing"
)

var testParams *kvparams.CosmosDB

func TestMain(m *testing.M) {
	testParams = &kvparams.CosmosDB{
		ReadWriteKey: "Y2Z65CW7lNe1OYFVErwcIRBSgvxCGgJWhFBgFsL22KukCBddDGFc2EtRrDLjnKYPcjLKZQwvK7iaACDbFE5DIA==",
		Endpoint:     "https://esti-e2e-tests.documents.azure.com:443/",
		Database:     "esti-db",
		Container:    "aaa",
	}
	code := m.Run()
	os.Exit(code)
}

//func TestMain(m *testing.M) {
//	databaseURI, cleanupFunc, err := testutil.GetCosmosDBInstance()
//	if err != nil {
//		log.Fatalf("Could not connect to Docker: %s", err)
//	}
//
//	testParams = &kvparams.CosmosDB{
//		Endpoint:     databaseURI,
//		ReadWriteKey: "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
//		Database:     "test-db",
//		Container:    "test-container",
//		Client: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
//			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // ignore self-signed cert for local testing using the emulator
//		}},
//		StrongConsistency: false,
//	}
//
//	cred, err := azcosmos.NewKeyCredential(testParams.ReadWriteKey)
//	if err != nil {
//		log.Fatalf("creating key: %v", err)
//	}
//	// Create a CosmosDB client
//	client, err := azcosmos.NewClientWithKey(testParams.Endpoint, cred, &azcosmos.ClientOptions{
//		ClientOptions: azcore.ClientOptions{
//			Transport: &http.Client{Transport: &http.Transport{
//				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
//			}},
//		},
//	})
//	if err != nil {
//		log.Fatalf("creating client using access key: %v", err)
//	}
//
//	log.Printf("Creating database %s", testParams.Database)
//
//	ctx := context.Background()
//	throughput := azcosmos.NewManualThroughputProperties(4000)
//	resp, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: testParams.Database},
//		&azcosmos.CreateDatabaseOptions{ThroughputProperties: &throughput})
//	if err != nil {
//		print(resp.RawResponse)
//		log.Fatalf("creating database: %v", err)
//	}
//
//	databaseClient, err := client.NewDatabase(testParams.Database)
//	if err != nil {
//		log.Fatalf("creating database client: %v", err)
//	}
//
//	log.Printf("Creating container %s", testParams.Container)
//	resp2, err := databaseClient.CreateContainer(ctx, azcosmos.ContainerProperties{
//		ID: testParams.Container,
//		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
//			Paths: []string{"/partitionKey"},
//		},
//	}, &azcosmos.CreateContainerOptions{ThroughputProperties: &throughput})
//	if err != nil {
//		print(resp2.RawResponse)
//		log.Fatalf("creating container: %v", err)
//	}
//
//	code := m.Run()
//	cleanupFunc()
//	os.Exit(code)
//}
