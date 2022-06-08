package testutil

import (
	"fmt"
	"net/http"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	DefaultDynamodbLocalPort  = "6432"

	WaitForContainerSec = 2
)

func RunLocalDynamoDBInstance(port string) (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}

	if len(port) == 0 {
		port = DefaultDynamodbLocalPort
	}

	dynamodbDockerRunOptions := &dockertest.RunOptions{
		Repository: "amazon/dynamodb-local",
		Tag:        "latest",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8000/tcp": {{HostPort: port}},
		},
	}

	resource, err := dockerPool.RunWithOptions(dynamodbDockerRunOptions)
	if err != nil {
		return "", nil, fmt.Errorf("could not start dynamodb local: %w", err)
	}

	// set cleanup
	closer := func() {
		err := dockerPool.Purge(resource)
		if err != nil {
			panic("could not kill dynamodb local container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		return "", nil, fmt.Errorf("could not expire dynamodb local container: %w", err)
	}

	uri := fmt.Sprintf("http://localhost:%s", port)

	err = dockerPool.Retry(func() error {
		// waiting for dynamodb container to be ready by issuing an http get request with
		// exponential backoff retry. The response is not really meaningful for that case
		// and so is ignored
		_, err := http.Get(uri)
		return err
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to dynamodb at %s: %w", uri, err)
	}

	// return DB URI
	return uri, closer, nil
}
