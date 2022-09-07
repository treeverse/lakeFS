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
	awsConfig *aws.Config
}

func NewMetadataProvider(logger logging.Logger, awsConfig *aws.Config) *MetadataProvider {
	return &MetadataProvider{logger: logger, awsConfig: awsConfig}
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	// max number of retries on the client operation
	const sessionMaxRetries = 0
	// use a shorter timeout than default because the service can be inaccessible from networks which don't have internet connection
	const sessionTimeout = 5 * time.Second
	metaConfig := &aws.Config{
		HTTPClient: &http.Client{
			Timeout: sessionTimeout,
		},
		MaxRetries: aws.Int(sessionMaxRetries),
	}
	sess, err := session.NewSession(m.awsConfig.Copy(metaConfig))
	if err != nil {
		m.logger.WithError(err).Warn("Failed to create AWS session for BI")
		return nil
	}
	sess.ClientConfig(s3.ServiceName)

	stsClient := sts.New(sess)

	const (
		maxInterval    = 200 * time.Millisecond
		maxElapsedTime = 3 * time.Second
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxInterval
	bo.MaxElapsedTime = maxElapsedTime
	var identity *sts.GetCallerIdentityOutput
	err = backoff.Retry(func() error {
		identity, err = stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
		if err != nil {
			m.logger.WithError(err).Warn("Tried to to get AWS account ID for BI")
			return err
		}
		return nil
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
