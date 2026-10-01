package aws

import (
	"context"
	"net/http"

	"github.com/Shopify/go-lua"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Open registers the aws library.  Clients send requests using transport, the SDK default when nil.
func Open(l *lua.State, ctx context.Context, transport http.RoundTripper) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "s3_client", Function: newS3Client(ctx, transport)},
			{Name: "glue_client", Function: newGlueClient(ctx, transport)},
		})
		return 1
	}
	lua.Require(l, "aws", open, false)
	l.Pop(1)
}

// loadConfig returns the SDK configuration for the script-supplied credentials.
func loadConfig(ctx context.Context, region, accessKeyID, secretAccessKey string, transport http.RoundTripper) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	}
	if transport != nil {
		opts = append(opts, config.WithHTTPClient(&http.Client{Transport: transport}))
	}
	return config.LoadDefaultConfig(ctx, opts...)
}
