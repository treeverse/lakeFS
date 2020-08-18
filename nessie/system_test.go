// +build systemtests

package nessie

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	genclient "github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/setup"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

// testsConfig contains configuration needed for running the tests
type testsConfig struct {
	// baseURL is the base address of the lakeFS endpoint
	baseURL string

	// rawBucketPath is the full path to the s3 bucket
	rawBucketPath string

	// maxSetup is the maximum time to wait for lakeFS setup
	maxSetup time.Duration
}

var (
	config testsConfig
	logger logging.Logger
	client api.Client
)

const (
	masterBranch = "master"
)

func init() {
	flag.StringVar(&config.baseURL, "endpoint-url", "http://localhost:8000", "URL endpoint of the lakeFS instance")
	flag.StringVar(&config.rawBucketPath, "bucket", "s3://nessie-system-testing", "Bucket's path")
	flag.DurationVar(&config.maxSetup, "max-setup", 5*time.Minute, "Maximum time to wait for lakeFS setup")
}

func TestMain(m *testing.M) {
	flag.Parse()
	logger = logging.Default()
	ctx := context.Background()

	// initialize the env/repo
	logger = logging.Default()
	logger.WithField("bucketPath", config.rawBucketPath).Info("Starting nessie run")

	url, err := url.Parse(config.baseURL)
	if err != nil {
		panic(fmt.Errorf("failed to parse url %s: %w", config.baseURL, err))
	}

	cl := genclient.NewHTTPClientWithConfig(strfmt.Default, &genclient.TransportConfig{
		Host:     url.Host,
		BasePath: genclient.DefaultBasePath,
		Schemes:  []string{url.Scheme},
	})

	// first setup of lakeFS
	setupCtx, _ := context.WithTimeout(ctx, config.maxSetup)
	for {
		_, err := cl.HealthCheck.HealthCheck(nil)
		if err == nil {
			break
		}
		logger.WithError(err).Info("Setup failed")

		select {
		case <-setupCtx.Done():
			panic("setup failed after all retries")
		case <-time.After(5 * time.Second):
		}
	}

	adminUserName := "nessie"
	res, err := cl.Setup.SetupLakeFS(&setup.SetupLakeFSParams{
		User: &models.Setup{
			DisplayName: &adminUserName,
		},
		Context: ctx,
	})
	if err != nil {
		panic(fmt.Errorf("lakefs setup request failed: %w", err))
	}

	logger.WithField("accessKeyID", res.Payload.AccessKeyID).
		WithField("accessSecretKey", res.Payload.AccessSecretKey).
		Info("Cluster setup successfully")

	client, err = api.NewClient(fmt.Sprintf("%s/%s", config.baseURL, genclient.DefaultBasePath), res.Payload.AccessKeyID, res.Payload.AccessSecretKey)
	if err != nil {
		panic(fmt.Errorf("failed to setup client: %w", err))
	}

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}

const (
	objPath       = "1.txt"
	contentLength = 16
)

func TestSingleCommit(t *testing.T) {
	ctx := context.Background()

	repo := createRepo(ctx, t)
	logger.WithField("repo", repo).Info("Created first repository")

	objContent := randstr.Hex(contentLength)
	_, err := client.UploadObject(ctx, repo, masterBranch, objPath, strings.NewReader(objContent))
	require.NoError(t, err, "failed to upload file")

	_, err = client.Commit(ctx, repo, masterBranch, "nessie:singleCommit", nil)
	require.NoError(t, err, "failed to commit changes")

	var b bytes.Buffer
	_, err = client.GetObject(ctx, repo, masterBranch, objPath, &b)
	require.NoError(t, err, "failed to get object")

	require.Equal(t, objContent, b.String(), fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, b.String()))
}

func createRepo(ctx context.Context, t *testing.T) string {
	name := strings.ToLower(t.Name())
	repoPath := config.rawBucketPath + "/" + name

	err := client.CreateRepository(ctx, &models.RepositoryCreation{
		DefaultBranch:    masterBranch,
		ID:               swag.String(name),
		StorageNamespace: swag.String(repoPath),
	})
	require.NoError(t, err, "failed to create repo")

	return name
}
