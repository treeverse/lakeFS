package gs_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/api/option"
)

const bucketName = "bucket1"

var client *storage.Client

func TestMain(m *testing.M) {
	const (
		emulatorContainerTimeoutSeconds = 10 * 60 // 10 min
		emulatorTestEndpoint            = "127.0.0.1"
		emulatorTestPort                = "4443"
		gcsProjectID                    = "testProject"
	)

	ctx := context.Background()
	// External port required for '-public-host' configuration in docker cmd
	endpoint := fmt.Sprintf("%s:%s", emulatorTestEndpoint, emulatorTestPort)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "fsouza/fake-gcs-server",
		Tag:        "1.47.4",
		Cmd: []string{
			"-scheme",
			"http",
			"-backend",
			"memory",
			"-public-host",
			endpoint,
		},
		ExposedPorts: []string{emulatorTestPort},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(fmt.Sprintf("%s/tcp", emulatorTestPort)): {
				{HostIP: emulatorTestPort, HostPort: fmt.Sprintf("%s/tcp", emulatorTestPort)},
			},
		},
	})
	if err != nil {
		log.Fatalf("Could not start fake-gcs-server: %s", err)
	}

	// set cleanup
	closer := func() {
		err = pool.Purge(resource)
		if err != nil {
			log.Fatalf("Could not purge fake-gcs-server: %s", err)
		}
	}

	// expire, just to make sure
	err = resource.Expire(emulatorContainerTimeoutSeconds)
	if err != nil {
		log.Fatalf("Could not expire fake-gcs-server: %s", err)
	}

	// Create the test client and bucket
	blockURL := fmt.Sprintf("http://%s/storage/v1/", endpoint)
	client, err = storage.NewClient(ctx, option.WithEndpoint(blockURL), option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Could not create gs client: %s", err)
	}

	if err := client.Bucket(bucketName).Create(ctx, gcsProjectID, nil); err != nil {
		log.Fatalf("Could not create bucket '%s': %s", bucketName, err)
	}

	code := m.Run()
	closer()
	os.Exit(code)
}
