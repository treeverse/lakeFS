package testutil

import (
	"fmt"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	DefaultDynamodbLocalPort  = "6432"
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
			"8000/tcp": []dc.PortBinding{{HostPort: port}},
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

	// Disgusting, but I have to wait for the dynamodb container to be ready
	time.Sleep(2 * time.Second)

	// return DB URI
	return uri, closer, nil
}
