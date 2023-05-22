package testutil

import (
	"fmt"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"net/http"
)

const (
	CosmosDBLocalPort = "8081"

	cosmosdbLocalURI = "http://localhost:8081"
)

func GetCosmosDBInstance() (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}

	cosmosdbDockerRunOptions := &dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8081/tcp": {{HostPort: CosmosDBLocalPort}},
		},
		Env: []string{"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=10", "AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=true"},
	}

	resource, err := dockerPool.RunWithOptions(cosmosdbDockerRunOptions)
	if err != nil {
		return "", nil, fmt.Errorf("could not start cosmosdb emulator: %w", err)
	}

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
		resp, err := http.Get(cosmosdbLocalURI)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to cosmosdb emulator at %s: %w", cosmosdbLocalURI, err)
	}

	// return DB URI
	return cosmosdbLocalURI, closer, nil
}
