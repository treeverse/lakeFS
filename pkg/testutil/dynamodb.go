package testutil

import (
	"fmt"
	"net/http"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	DynamodbLocalPort         = "6432"
	DynamodbLocalURI          = "http://localhost:6432"

	WaitForContainerSec = 2
)

func RunLocalDynamoDBInstance() (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}

	dynamodbDockerRunOptions := &dockertest.RunOptions{
		Repository: "amazon/dynamodb-local",
		Tag:        "latest",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8000/tcp": {{HostPort: DynamodbLocalPort}},
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

	err = dockerPool.Retry(func() error {
		// waiting for dynamodb container to be ready by issuing an http get request with
		// exponential backoff retry. The response is not really meaningful for that case
		// and so is ignored
		resp, err := http.Get(DynamodbLocalURI)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to dynamodb at %s: %w", DynamodbLocalURI, err)
	}

	// return DB URI
	return DynamodbLocalURI, closer, nil
}
