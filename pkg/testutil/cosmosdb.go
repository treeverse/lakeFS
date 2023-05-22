package testutil

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

const (
	CosmosDBLocalPort = "8081"
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
		Env: []string{"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=10",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=true",
			"AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8081/tcp": {{HostPort: CosmosDBLocalPort}},
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
			panic("could not kill cosmosdb local container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		return "", nil, fmt.Errorf("could not expire cosmosdb local emulator: %w", err)
	}

	err = dockerPool.Retry(func() error {
		// waiting for cosmosdb container to be ready by issuing an HTTP get request with
		// exponential backoff retry. The response is not really meaningful for that case
		// and so is ignored
		p, err := url.JoinPath(cosmosdbLocalURI, "/_explorer/emulator.pem")
		if err != nil {
			log.Fatalf("failed joining urls: %v", err)
		}
		client := http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(p)
		if err != nil {
			log.Printf("failed probing cosmos db(%s): %v", p, err)
			return err
		}
		_ = resp.Body.Close()
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to cosmosdb emulator at %s: %w", cosmosdbLocalURI, err)
	}

	log.Printf("cosmosdb emulator is ready at %s", cosmosdbLocalURI)

	// return DB URI
	return cosmosdbLocalURI, closer, nil
}
