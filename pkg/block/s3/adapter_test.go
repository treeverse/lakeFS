package s3_test

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/s3"
)

func TestS3Adapter(t *testing.T) {
	basePath, err := url.JoinPath("s3://", bucketName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	sess := session.Must(session.NewSession(
		aws.NewConfig().WithCredentials(credentials.NewCredentials(
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     minioTestAccessKeyID,
					SecretAccessKey: minioTestSecretAccessKey,
				},
			})).WithEndpoint(blockURL).
			WithRegion("us-east-1").
			WithS3ForcePathStyle(true)),
	)
	adapter := s3.NewAdapter(sess, s3.WithDiscoverBucketRegion(false))
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}
