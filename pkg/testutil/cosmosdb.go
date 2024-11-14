package testutil

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/ory/dockertest/v3"
)

const (
	CosmosDBLocalPort = "8081"
	maxWait           = 5 * time.Minute // Cosmosdb emulator takes time to start
)

var cosmosdbLocalURI string

func GetCosmosDBInstance() (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}

	cosmosdbDockerRunOptions := &dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		Env: []string{
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=100",
		},
		ExposedPorts: []string{CosmosDBLocalPort},
	}

	resource, err := dockerPool.RunWithOptions(cosmosdbDockerRunOptions)
	if err != nil {
		return "", nil, fmt.Errorf("could not start cosmosdb emulator: %w", err)
	}

	cosmosdbLocalURI = "https://localhost:" + resource.GetPort("8081/tcp")
	// set cleanup
	closer := func() {
		err = dockerPool.Purge(resource)
		if err != nil {
			fmt.Println("could not kill cosmosdb local container :%w", err)
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		defer closer() // defer so that error is logged appropriately
		return "", nil, fmt.Errorf("could not expire cosmosdb local emulator: %w", err)
	}
	p, err := url.JoinPath(cosmosdbLocalURI, "/_explorer/emulator.pem")
	if err != nil {
		defer closer()
		return "", nil, fmt.Errorf("joining urls: %w", err)
	}

	dockerPool.MaxWait = maxWait
	log.Printf("Waiting up to %v for emulator to start", dockerPool.MaxWait)
	err = dockerPool.Retry(func() error {
		// waiting for cosmosdb container to be ready by issuing an HTTP get request with
		// exponential backoff retry. The response is not really meaningful for that case
		// and so is ignored
		client := http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // ignore self-signed cert for local testing using the emulator
		}}
		resp, err := client.Get(p)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	})
	if err != nil {
		defer closer()
		return "", nil, fmt.Errorf("could not connect to cosmosdb emulator at %s: %w", cosmosdbLocalURI, err)
	}

	return cosmosdbLocalURI, closer, nil
}
