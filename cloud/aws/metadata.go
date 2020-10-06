package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/treeverse/lakefs/logging"
)

type MetadataProvider struct {
	logger    logging.Logger
	awsConfig *aws.Config
}

func NewMetadataProvider(logger logging.Logger, awsConfig *aws.Config) *MetadataProvider {
	return &MetadataProvider{logger: logger, awsConfig: awsConfig}
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	sess, err := session.NewSession(m.awsConfig)
	if err != nil {
		m.logger.Warnf("%v: failed to create AWS session for BI", err)
		return nil
	}
	sess.ClientConfig(s3.ServiceName)
	stsClient := sts.New(sess)
	identity, err := stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		m.logger.Warnf("%v: failed to get AWS account ID for BI", err)
		return nil
	}
	return map[string]string{"aws_account_id": *identity.Account}
}
