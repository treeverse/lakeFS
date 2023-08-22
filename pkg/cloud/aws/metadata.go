package aws

import (
	"context"
	"crypto/md5" //nolint:gosec
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataProvider struct {
	logger    logging.Logger
	stsClient *sts.Client
}

func NewMetadataProvider(logger logging.Logger, awsConfig aws.Config) *MetadataProvider {
	// set up a session with a shorter timeout and no retries
	const sessionMaxRetries = 0 // max number of retries on the client operation

	// use a shorter timeout than default
	// because the service can be inaccessible from networks
	// which don't have an internet connection
	const sessionTimeout = 5 * time.Second
	stsClient := sts.NewFromConfig(awsConfig, func(options *sts.Options) {
		options.RetryMaxAttempts = sessionMaxRetries
		options.HTTPClient = &http.Client{
			Timeout: sessionTimeout,
		}
	})
	return &MetadataProvider{logger: logger, stsClient: stsClient}
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
	identity, err := backoff.RetryWithData(func() (*sts.GetCallerIdentityOutput, error) {
		identity, err := m.stsClient.GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
		if err != nil {
			m.logger.WithError(err).Warn("Tried to to get AWS account ID for BI")
			return nil, err
		}
		return identity, nil
	}, bo)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to to get AWS account ID for BI")
		return nil
	}

	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(*identity.Account))), //nolint:gosec
		cloud.IDTypeKey: "aws_account_id",
	}
}
