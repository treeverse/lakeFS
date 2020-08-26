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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/api"
	genclient "github.com/treeverse/lakefs/api/gen/client"
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

	// maxSetup is the maximum time to wait for lakeFS setup
	maxSetup time.Duration

	// gatewayDomainName is the lakeFS host configuration for accepting gateway requests
	gatewayDomainName string
}

var (
	config testsConfig
	logger logging.Logger
	client api.Client
	svc    *s3.S3
)

const (
	masterBranch = "master"
)

func init() {
	flag.StringVar(&config.baseURL, "endpoint-url", "http://localhost:8000", "URL endpoint of the lakeFS instance")
	flag.StringVar(&config.rawBucketPath, "bucket", "s3://nessie-system-testing", "Bucket's path")
	flag.StringVar(&config.gatewayDomainName, "gw-domain-name", "s3.local.lakefs.io:8000", "Gateway domain name")
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
	waitUntilLakeFSRunning(ctx, cl)
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

	awsSession := session.Must(session.NewSession())
	svc = s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithEndpoint(config.gatewayDomainName).
			WithDisableSSL(true).
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     res.Payload.AccessKeyID,
						SecretAccessKey: res.Payload.AccessSecretKey,
					}})))

	logger.Info("Setup succeeded, running the tests")
	os.Exit(m.Run())
}

func waitUntilLakeFSRunning(ctx context.Context, cl *genclient.Lakefs) {
	setupCtx, cancel := context.WithTimeout(ctx, config.maxSetup)
	defer cancel()
	for {
		_, err := cl.HealthCheck.HealthCheck(nil)
		if err == nil {
			break
		}
		logger.WithError(err).Info("Setup failed")

		select {
		case <-setupCtx.Done():
			panic("health check failed after all retries")
		case <-time.After(5 * time.Second):
		}
	}
}

const (
	contentLength = 16
)

func TestSingleCommit(t *testing.T) {
	ctx, _, repo := setupTest(t)
	objPath := "1.txt"

	_, objContent := uploadFile(ctx, t, repo, masterBranch, objPath)
	_, err := client.Commit(ctx, repo, masterBranch, "nessie:singleCommit", nil)
	require.NoError(t, err, "failed to commit changes")

	var b bytes.Buffer
	_, err = client.GetObject(ctx, repo, masterBranch, objPath, &b)
	require.NoError(t, err, "failed to get object")

	require.Equal(t, objContent, b.String(), fmt.Sprintf("path: %s, expected: %s, actual:%s", objPath, objContent, b.String()))
}

func TestMergeAndList(t *testing.T) {
	ctx, logger, repo := setupTest(t)
	branch := "feature-1"

	ref, err := client.CreateBranch(ctx, repo, &models.BranchCreation{
		Name:   swag.String(branch),
		Source: swag.String(masterBranch),
	})
	require.NoError(t, err, "failed to create branch")
	logger.WithField("branchRef", ref).Info("Created branch, committing files")

	numberOfFiles := 10
	checksums := map[string]string{}
	for i := 0; i < numberOfFiles; i++ {
		checksum, content := uploadFile(ctx, t, repo, branch, fmt.Sprintf("%d.txt", i))
		checksums[checksum] = content
	}

	_, err = client.Commit(ctx, repo, branch, fmt.Sprintf("Adding %d files", numberOfFiles), nil)
	require.NoError(t, err, "failed to commit changes")

	mergeRes, err := client.Merge(ctx, repo, masterBranch, branch)
	require.NoError(t, err, "failed to merge branches")
	logger.WithField("mergeResult", mergeRes).Info("Merged successfully")

	objs, pagin, err := client.ListObjects(ctx, repo, masterBranch, "", "", 100)
	require.NoError(t, err, "failed to list objects")
	require.False(t, *pagin.HasMore, "pagination shouldn't have more items")
	require.Equal(t, int64(numberOfFiles), *pagin.Results)
	require.Equal(t, numberOfFiles, len(objs))
	logger.WithField("objs", objs).WithField("pagin", pagin).Info("Listed successfully")

	for _, obj := range objs {
		_, ok := checksums[obj.Checksum]
		require.True(t, ok, "file exists in master but shouldn't, obj: %s", *obj)
	}
}

func setupTest(t *testing.T) (context.Context, logging.Logger, string) {
	ctx := context.Background()
	logger := logger.WithField("testName", t.Name())
	repo := createRepo(ctx, t)
	logger.WithField("repo", repo).Info("Created repository")

	return ctx, logger, repo
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

func uploadFile(ctx context.Context, t *testing.T, repo, branch, objPath string) (checksum, content string) {
	objContent := randstr.Hex(contentLength)
	stats, err := client.UploadObject(ctx, repo, branch, objPath, strings.NewReader(objContent))
	require.NoError(t, err, "failed to upload file")
	return stats.Checksum, objContent
}
