package dynamodb_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	DynamodbLocalPort         = "6432"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func runDBInstance(dockerPool *dockertest.Pool) (string, func()) {
	dynamodbDockerRunOptions := &dockertest.RunOptions{
		Repository: "amazon/dynamodb-local",
		Tag:        "latest",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8000/tcp": []dc.PortBinding{{HostPort: DynamodbLocalPort}},
		},
	}
	resource, err := dockerPool.RunWithOptions(dynamodbDockerRunOptions)
	if err != nil {
		panic("Could not start dynamodb local: " + err.Error())
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
		panic("could not expire dynamodb local container")
	}

	uri := fmt.Sprintf("http://localhost:%s", DynamodbLocalPort)

	// return DB URI
	return uri, closer
}

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var cleanup func()
	databaseURI, cleanup = runDBInstance(pool)
	code := m.Run()
	cleanup()
	os.Exit(code)
}
