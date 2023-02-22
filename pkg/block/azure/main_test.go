package azure_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/ory/dockertest/v3"
	"github.com/txn2/txeh"
)

const (
	azuriteContainerTimeoutSeconds = 10 * 60 // 10 min
	containerName                  = "container1"
	accountName                    = "account1"
	accountKey                     = "key1"
)

var (
	pool     *dockertest.Pool
	blockURL string
)

func runAzurite(dockerPool *dockertest.Pool) (string, func()) {
	ctx := context.Background()
	resource, err := dockerPool.Run("mcr.microsoft.com/azure-storage/azurite", "latest", []string{
		fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", accountName, accountKey),
	})

	accountHost := fmt.Sprintf("%s.blob.localhost", accountName)

	hosts, err := txeh.NewHostsDefault()
	if err != nil {
		panic(err)
	}
	hosts.AddHost("127.0.0.1", accountHost)

	// set cleanup
	closer := func() {
		hosts.RemoveHost(accountHost)
		err := dockerPool.Purge(resource)
		if err != nil {
			panic("could not kill postgres containerName")
		}
	}

	// expire, just to make sure
	err = resource.Expire(azuriteContainerTimeoutSeconds)
	if err != nil {
		panic("could not expire postgres containerName")
	}

	// create connection
	port := resource.GetPort("10000/tcp")
	url := fmt.Sprintf("http://%s:%s", accountHost, port)
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	client, err := service.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		panic(err)
	}

	// Check connectivity with containerName
	_, err = client.GetAccountInfo(ctx, nil)
	if err != nil {
		panic(err)
	}

	// Create test container
	blob, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		panic(err)
	}
	_, err = blob.CreateContainer(ctx, containerName, nil)
	if err != nil {
		panic(err)
	}

	// return DB URI
	return url, closer
}

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var cleanup func()
	blockURL, cleanup = runAzurite(pool)
	code := m.Run()
	cleanup()
	os.Exit(code)
}
