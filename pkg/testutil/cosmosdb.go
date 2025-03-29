package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var cosmosdbLocalURI string

var (
	ErrContainerExited  = errors.New("container exited")
	ErrEmulatorNotReady = errors.New("emulator not ready")
)

func GetCosmosDBInstance() (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}

	cosmosdbDockerRunOptions := &dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "vnext-preview",
		Env: []string{
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=100",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false",
			"AZURE_COSMOS_EMULATOR_DISABLE_THROTTLING=true",
			"ENABLE_TELEMETRY=false",
			"PROTOCOL=http",
		},
		ExposedPorts: []string{
			"8081/tcp",
			"1234/tcp",
		},
	}

	resource, err := dockerPool.RunWithOptions(cosmosdbDockerRunOptions)
	if err != nil {
		return "", nil, fmt.Errorf("could not start cosmosdb emulator: %w", err)
	}

	// explorer endpoint to verify when container is up
	cosmosdbExplorerURI := "http://localhost:" + resource.GetPort("1234/tcp")
	// set cleanup
	closer := func() {
		// Fetch logs from the container
		var containerOut bytes.Buffer
		if err := dockerPool.Client.Logs(docker.LogsOptions{
			Container:    resource.Container.ID,
			OutputStream: &containerOut,
			ErrorStream:  &containerOut,
			Stdout:       true,
			Stderr:       true,
			Follow:       false,
		}); err != nil {
			log.Printf("Error in cosmosdb emulator logs: %s", err)
		} else {
			log.Printf("CosmosDB emulator output: %s", containerOut.String())
		}

		if err := dockerPool.Purge(resource); err != nil {
			log.Printf("could not kill cosmosdb local container: %s", err)
		}
	}
	// cleanup is called when we return without databaseURI
	defer func() {
		if cosmosdbLocalURI == "" {
			closer()
		}
	}()

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		return "", nil, fmt.Errorf("could not expire cosmosdb local emulator: %w", err)
	}

	const clientTimeout = 5 * time.Second
	httpClient := http.Client{
		Timeout: clientTimeout,
	}

	// waiting for cosmosdb container to be ready by issuing an HTTP get request with
	// exponential backoff retry. The response is not really meaningful for that case
	// and so is ignored
	err = dockerPool.Retry(func() error {
		// Check if the container is still running
		container, err := dockerPool.Client.InspectContainer(resource.Container.ID)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("could not inspect container: %w", err))
		}
		if !container.State.Running {
			return backoff.Permanent(fmt.Errorf("%w with status: %s", ErrContainerExited, container.State.Status))
		}
		// Check if the cosmosdb emulator is up and running
		resp, err := httpClient.Get(cosmosdbExplorerURI)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("cosmosdb %w: %s", ErrEmulatorNotReady, resp.Status)
		}
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to cosmosdb emulator at %s: %w", cosmosdbLocalURI, err)
	}

	// cosmosdb emulator is running on port 8081.
	// set the URI last as the cleanup occur in case we fail before.
	cosmosdbLocalURI = "http://localhost:" + resource.GetPort("8081/tcp")

	return cosmosdbLocalURI, closer, nil
}
