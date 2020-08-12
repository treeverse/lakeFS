package nessie

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/logging"
	"strings"
	"time"
)

// Config contains configuration needed for running the tests
type Config struct {
	// BaseURL is the base address of the lakeFS endpoint
	BaseURL string

	// BucketPath is the full path to the s3 bucket
	BucketPath string
}

//
type testCase func(context.Context, api.Client) error


var (
	adminUserName string = "nessie"
)

// Run runs system tests and reports on failures
func Run(ctx context.Context, config Config) error {
	// initialize the env/repo
	logger := logging.FromContext(ctx)
	bucketPath := fmt.Sprintf("%s/%s",config.BucketPath, time.Now().Format("2006-01-02T15:04:05"))
	logger.WithField("bucketPath", bucketPath).Info("Starting nessie run")

	cl := client.NewHTTPClientWithConfig(strfmt.Default, &client.TransportConfig{
		Host:     config.BaseURL,
		BasePath: client.DefaultBasePath,
		Schemes: []string{"http"},
	})

	expBackoff := backoff.NewExponentialBackOff()
	if err := backoff.Retry(func() error {
		_, err := cl.HealthCheck.HealthCheck(nil)
		if err != nil {
			return err
		}
		return nil
	}, expBackoff); err != nil {
		return fmt.Errorf("api endpoint isn't ready: %w", err)
	}

	res, err := cl.Setup.SetupLakeFS(&setup.SetupLakeFSParams{
		User: &models.Setup{
			DisplayName: &adminUserName,
		},
		Context: ctx,
	})
	if err != nil {
		return fmt.Errorf("lakefs setup request failed: %w", err)
	}

	c, err := api.NewClient(fmt.Sprintf("http://%s/%s",config.BaseURL, client.DefaultBasePath), res.Payload.AccessKeyID, res.Payload.AccessSecretKey)
	if err != nil {
		return fmt.Errorf("failed to setup client: %w", err)
	}

	if err := c.CreateRepository(ctx, &models.RepositoryCreation{
		DefaultBranch:    "master",
		ID:               swag.String("my-repo"),
		StorageNamespace: &bucketPath,
	}); err != nil {
		return fmt.Errorf("failed to create repo: %w", err)
	}

	// TODO(itai): run the tests in parallel
	if err := singleCommit(ctx, c); err != nil{
		return err
	}

	return nil
}

func singleCommit(ctx context.Context, client api.Client) error{
	objContent := "my request"
	objPath := "single-commit/1.txt"
	_, err := client.UploadObject(ctx, "my-repo", "master", objPath, strings.NewReader(objContent))
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	_, err = client.Commit(ctx, "my-repo", "master", "nessie:singleCommit", nil)
	if err != nil {
		return fmt.Errorf("failed to commit changes: %w", err)
	}

	var b bytes.Buffer
	_, err = client.GetObject(ctx, "my-repo", "master", objPath, &b)
	if err != nil{
		return fmt.Errorf("failed to get object: %w", err)
	}
	if objContent != b.String() {
		return fmt.Errorf("%s object content mismatch: expected: %s, actual:%s", objPath,objContent, b.String())
	}

	return nil
}
