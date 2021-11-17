package aws

import (
	"context"
	"crypto/md5" //nolint:gosec
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataProvider struct {
	logger    logging.Logger
	awsConfig *aws.Config
}

func NewMetadataProvider(logger logging.Logger, awsConfig *aws.Config) *MetadataProvider {
	return &MetadataProvider{logger: logger, awsConfig: awsConfig}
}

func (m *MetadataProvider) GetMetadata(ctx context.Context) map[string]string {
	// max number of retries on the client operation
	const sessionMaxAttempts = 0
	// use a shorter timeout than default because the service can be inaccessible from networks which don't have internet connection
	const sessionTimeout = 5 * time.Second
	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = sessionMaxAttempts
	})
	stsClient := sts.New(sts.Options{
		HTTPClient: &http.Client{
			Timeout: sessionTimeout,
		},
		Retryer: retryer,
	})
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		m.logger.WithError(err).Warn("Failed to get AWS account ID for BI")
		return nil
	}
	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(*identity.Account))), //nolint:gosec
		cloud.IDTypeKey: "aws_account_id",
	}
}
