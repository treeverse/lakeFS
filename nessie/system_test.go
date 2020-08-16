package nessie

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	genclient "github.com/treeverse/lakefs/api/gen/client"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
)

// testsConfig contains configuration needed for running the tests
type testsConfig struct {
	// baseURL is the base address of the lakeFS endpoint
	baseURL string

	// rawBucketPath is the full path to the s3 bucket
	rawBucketPath string

	enabled bool
	https   bool
}

var (
	config testsConfig
	logger logging.Logger
	client api.Client

	// full path to the test directory under the s3 bucket
	testBucketPath string
)

const (
	maxSetup     = 5 * time.Minute
	masterBranch = "master"
)

func init() {
	flag.StringVar(&config.baseURL, "endpoint-url", "localhost:8000", "URL endpoint of the lakeFS instance")
	flag.StringVar(&config.rawBucketPath, "bucket", "s3://nessie-system-testing", "Bucket's path")
	flag.BoolVar(&config.enabled, "system-tests", false, "Enabling the system-tests")
	flag.BoolVar(&config.https, "https", false, "Use HTTPS to connect to lakeFS")
}

func TestMain(m *testing.M) {
	flag.Parse()

	if !config.enabled {
		// system tests disabled
		os.Exit(0)
	}

	// initialize the env/repo
	logger = logging.Default()
	testBucketPath = fmt.Sprintf("%s/%s", config.rawBucketPath, time.Now().UTC().Format("2006/01/02/150405"))
	logger.WithField("bucketPath", testBucketPath).Info("Starting nessie run")

	schemas := []string{"http"}
	if config.https {
		schemas = append(schemas, "https")
	}
	cl := genclient.NewHTTPClientWithConfig(strfmt.Default, &genclient.TransportConfig{
		Host:     config.baseURL,
		BasePath: genclient.DefaultBasePath,
		Schemes:  schemas,
	})

	ctx, _ := context.WithTimeout(context.Background(), maxSetup)
	for {
		_, err := cl.HealthCheck.HealthCheck(nil)
		if err == nil {
			break
		}

		logger.WithError(err).Info("Setup failed")

		select {
		case <-ctx.Done():
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

	client, err = api.NewClient(fmt.Sprintf("http://%s/%s", config.baseURL, genclient.DefaultBasePath), res.Payload.AccessKeyID, res.Payload.AccessSecretKey)
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
	repoPath := testBucketPath + "/" + name

	err := client.CreateRepository(ctx, &models.RepositoryCreation{
		DefaultBranch:    masterBranch,
		ID:               swag.String(name),
		StorageNamespace: swag.String(repoPath),
	})
	require.NoError(t, err, "failed to create repo")

	return name
}
