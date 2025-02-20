package aws

import (
	"context"
	"crypto/md5" //nolint:gosec
	"fmt"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataProvider struct {
	logger    logging.Logger
	stsClient *sts.Client
}

func NewMetadataProvider(logger logging.Logger, params params.S3) (*MetadataProvider, error) {
	// set up a session with a shorter timeout and no retries
	const sessionMaxRetries = 0 // max number of retries on the client operation

	// use a shorter timeout than default
	// because the service can be inaccessible from networks
	// which don't have an internet connection
	const sessionTimeout = 5 * time.Second
	/// params
	var opts []func(*config.LoadOptions) error
	if params.Region != "" {
		opts = append(opts, config.WithRegion(params.Region))
	}
	if params.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(params.Profile))
	}
	if params.CredentialsFile != "" {
		opts = append(opts, config.WithSharedCredentialsFiles([]string{params.CredentialsFile}))
	}
	if params.Credentials.AccessKeyID != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				params.Credentials.AccessKeyID,
				params.Credentials.SecretAccessKey,
				params.Credentials.SessionToken,
			),
		))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	stsClient := sts.NewFromConfig(cfg, func(options *sts.Options) {
		options.RetryMaxAttempts = sessionMaxRetries
		options.HTTPClient = awshttp.NewBuildableClient().
			WithTimeout(sessionTimeout)
	})
	return &MetadataProvider{logger: logger, stsClient: stsClient}, nil
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	if m.stsClient == nil {
		return nil
	}
	const (
		maxInterval    = 200 * time.Millisecond
		maxElapsedTime = 3 * time.Second
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxInterval
	bo.MaxElapsedTime = maxElapsedTime
	ctx := context.Background()
	identity, err := backoff.RetryWithData(func() (*sts.GetCallerIdentityOutput, error) {
		identity, err := m.stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			return nil, err
		}
		return identity, nil
	}, bo)
	if err != nil {
		m.logger.WithError(err).Trace("Failed to to get AWS account ID for BI")
		return nil
	}

	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(*identity.Account))), //nolint:gosec
		cloud.IDTypeKey: "aws_account_id",
	}
}
