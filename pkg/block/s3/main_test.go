package s3_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ory/dockertest/v3"
)

const (
	minioContainerTimeoutSeconds = 10 * 60 // 10 min
	bucketName                   = "bucket1"
	minioTestEndpoint            = "127.0.0.1"
	minioTestAccessKeyID         = "Q3AM3UQ867SPQQA43P2F"
	minioTestSecretAccessKey     = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
)

var (
	blockURL string
	pool     *dockertest.Pool
)

func newClient(port string) (*minio.Client, error) {
	creds := credentials.NewStaticV4(minioTestAccessKeyID, minioTestSecretAccessKey, "")

	client, err := minio.New(fmt.Sprintf("%s:%s", minioTestEndpoint, port), &minio.Options{
		Creds: creds,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "RELEASE.2023-06-09T07-32-12Z",
		Env: []string{
			fmt.Sprintf("MINIO_ROOT_USER=%s", minioTestAccessKeyID),
			fmt.Sprintf("MINIO_ROOT_PASSWORD=%s", minioTestSecretAccessKey),
		},
		Cmd: []string{
			"server",
			"start",
		},
	})
	if err != nil {
		panic(err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
		if err != nil {
			panic("could not purge minio container: " + err.Error())
		}
	}

	// expire, just to make sure
	err = resource.Expire(minioContainerTimeoutSeconds)
	if err != nil {
		panic("could not expire minio container: " + err.Error())
	}

	// Create test client and bucket
	client, err := newClient(resource.GetPort("9000/tcp"))
	if err != nil {
		log.Fatalf("create client: %s", err)
	}
	blockURL = client.EndpointURL().String()

	err = client.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{
		Region: "us-east-1",
	})
	if err != nil {
		log.Fatalf("create bucket: %s", err)
	}

	code := m.Run()
	closer()
	os.Exit(code)
}
