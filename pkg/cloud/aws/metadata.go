package aws

import (
	"crypto/md5" //nolint:gosec
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataProvider struct {
	logger    logging.Logger
	stsClient *sts.STS
}

func NewMetadataProvider(logger logging.Logger, awsConfig *aws.Config) *MetadataProvider {
	// set up a session with a shorter timeout and no retries
	const sessionMaxRetries = 0 // max number of retries on the client operation

	// use a shorter timeout than default
	// because the service can be inaccessible from networks
	// which don't have an internet connection
	const sessionTimeout = 5 * time.Second
	metaConfig := &aws.Config{
		HTTPClient: &http.Client{
			Timeout: sessionTimeout,
		},
		MaxRetries: aws.Int(sessionMaxRetries),
	}

	var stsClient *sts.STS
	sess, err := session.NewSession(awsConfig.Copy(metaConfig))
	if err != nil {
		logger.WithError(err).Warn("Failed to create AWS session for BI")
	} else {
		sess.ClientConfig(s3.ServiceName)
		stsClient = sts.New(sess)
	}

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
		identity, err := m.stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
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
