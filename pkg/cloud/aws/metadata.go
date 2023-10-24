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
	lg := logger.WithField("trace_flow", true).WithField("method", "NewMetadataProvider")
	lg.Info("start calling aws init md provider")
	// set up a session with a shorter timeout and no retries
	const sessionMaxRetries = 0 // max number of retries on the client operation

	// use a shorter timeout than default
	// because the service can be inaccessible from networks
	// which don't have an internet connection
	const sessionTimeout = 5 * time.Second
	/// params
	var opts []func(*config.LoadOptions) error
	if params.Region != "" {
		lg.Infof("using region %s", params.Region)
		opts = append(opts, config.WithRegion(params.Region))
	}
	if params.Profile != "" {
		lg.Infof("using profile %s", params.Profile)
		opts = append(opts, config.WithSharedConfigProfile(params.Profile))
	}
	if params.CredentialsFile != "" {
		lg.Infof("using credential file %s", params.CredentialsFile)
		opts = append(opts, config.WithSharedCredentialsFiles([]string{params.CredentialsFile}))
	}
	if params.Credentials.AccessKeyID != "" {
		lg.Info("using access key id")
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				params.Credentials.AccessKeyID,
				params.Credentials.SecretAccessKey,
				params.Credentials.SessionToken,
			),
		))
	}
	lg.Info("loading default aws config")
	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		lg.Info("failed loading default aws config")
		return nil, err
	}
	lg.Info("creating STS AWS Client")
	stsClient := sts.NewFromConfig(cfg, func(options *sts.Options) {
		lg.WithField("sesstion_timeout", sessionTimeout).Info("configurating options STS Client")
		options.RetryMaxAttempts = sessionMaxRetries
		options.HTTPClient = awshttp.NewBuildableClient().
			WithTimeout(sessionTimeout)
	})
	lg.Info("Done. STS Client created, returning MD Provider")
	return &MetadataProvider{logger: logger, stsClient: stsClient}, nil
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	lg := logging.ContextUnavailable().WithField("trace_flow", true).WithField("method", "GetMetadata")
	lg.Info("start calling aws init md provider")
	if m.stsClient == nil {
		lg.Info("no sts client return")
		return nil
	}
	const (
		maxInterval    = 200 * time.Millisecond
		maxElapsedTime = 3 * time.Second
	)
	lg = lg.WithField("max_internal_ms", maxInterval).WithField("max_elapsed_time_s", 3)
	lg.Info("will try get caller identity with retry")
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxInterval
	bo.MaxElapsedTime = maxElapsedTime
	ctx := context.Background()
	identity, err := backoff.RetryWithData(func() (*sts.GetCallerIdentityOutput, error) {
		lg.Info("retry attempt: GetCallerIdentity")
		identity, err := m.stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			m.logger.WithError(err).Warn("Tried to to get AWS account ID for BI")
			return nil, err
		}
		lg.Info("success getting identity: GetCallerIdentity")
		return identity, nil
	}, bo)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to to get AWS account ID for BI")
		return nil
	}
	lg.Info("finished getting MD return")
	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(*identity.Account))), //nolint:gosec
		cloud.IDTypeKey: "aws_account_id",
	}
}
