package aws

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/treeverse/lakefs/pkg/block/params"
)

func NewS3Client(params *params.AWSParams, opts ...func(*s3.Options)) *s3.Client {
	opts = append(opts, func(o *s3.Options) {
		o.UsePathStyle = params.S3.ForcePathStyle
	})
	if params.EndpointURL != "" {
		opts = append(opts, s3.WithEndpointResolver(s3.EndpointResolverFromURL(params.EndpointURL)))
	}
	return s3.NewFromConfig(*params.Config, opts...)
}
