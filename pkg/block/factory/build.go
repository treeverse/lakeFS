package factory

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/block/transient"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// AdapterOptions holds options for each adapter type that supports options
type AdapterOptions struct {
	Local []func(*local.Adapter)
	S3    []s3a.AdapterOption
	GS    []gs.AdapterOption
}

// BuildOption is a function that modifies the adapter options
type BuildOption func(*AdapterOptions)

// WithAdapterOptions returns a BuildOption that sets adapter-specific options
func WithAdapterOptions(opts AdapterOptions) BuildOption {
	return func(o *AdapterOptions) {
		if opts.Local != nil {
			o.Local = append(o.Local, opts.Local...)
		}
		if opts.S3 != nil {
			o.S3 = append(o.S3, opts.S3...)
		}
		if opts.GS != nil {
			o.GS = append(o.GS, opts.GS...)
		}
	}
}

const (
	// googleAuthCloudPlatform - Cloud Storage authentication https://cloud.google.com/storage/docs/authentication
	googleAuthCloudPlatform = "https://www.googleapis.com/auth/cloud-platform"
)

func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c config.AdapterConfig, opts ...BuildOption) (block.Adapter, error) {
	// Apply options
	options := &AdapterOptions{}
	for _, opt := range opts {
		opt(options)
	}

	blockstore := strings.ToLower(c.BlockstoreType())
	logging.FromContext(ctx).
		WithField("type", blockstore).
		Info("initialize blockstore adapter")
	switch blockstore {
	case block.BlockstoreTypeLocal:
		p, err := c.BlockstoreLocalParams()
		if err != nil {
			return nil, err
		}
		return buildLocalAdapter(ctx, p, options.Local...)
	case block.BlockstoreTypeS3:
		p, err := c.BlockstoreS3Params()
		if err != nil {
			return nil, err
		}
		return buildS3Adapter(ctx, statsCollector, p, options.S3...)
	case block.BlockstoreTypeMem, "memory":
		return mem.New(ctx), nil
	case block.BlockstoreTypeTransient:
		return transient.New(ctx), nil
	case block.BlockstoreTypeGS:
		p, err := c.BlockstoreGSParams()
		if err != nil {
			return nil, err
		}
		return buildGSAdapter(ctx, p, options.GS...)
	case block.BlockstoreTypeAzure:
		p, err := c.BlockstoreAzureParams()
		if err != nil {
			return nil, err
		}
		return azure.NewAdapter(ctx, p)
	default:
		return nil, fmt.Errorf("%w '%s' please choose one of %s",
			block.ErrInvalidAddress, blockstore, []string{block.BlockstoreTypeLocal, block.BlockstoreTypeS3, block.BlockstoreTypeAzure, block.BlockstoreTypeMem, block.BlockstoreTypeTransient, block.BlockstoreTypeGS})
	}
}

func buildLocalAdapter(ctx context.Context, params params.Local, adapterOpts ...func(*local.Adapter)) (*local.Adapter, error) {
	opts := []func(*local.Adapter){
		local.WithAllowedExternalPrefixes(params.AllowedExternalPrefixes),
		local.WithImportEnabled(params.ImportEnabled),
	}
	opts = append(opts, adapterOpts...)

	adapter, err := local.NewAdapter(params.Path, opts...)
	if err != nil {
		return nil, fmt.Errorf("got error opening a local block adapter with path %s: %w", params.Path, err)
	}
	logging.FromContext(ctx).WithFields(logging.Fields{
		"type": "local",
		"path": params.Path,
	}).Info("initialized blockstore adapter")
	return adapter, nil
}

func BuildS3Client(ctx context.Context, params params.S3) (*s3.Client, error) {
	cfg, err := s3a.LoadConfig(ctx, params)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, s3a.WithClientParams(params))
	return client, nil
}

func buildS3Adapter(ctx context.Context, statsCollector stats.Collector, params params.S3, adapterOpts ...s3a.AdapterOption) (*s3a.Adapter, error) {
	opts := []s3a.AdapterOption{
		s3a.WithStatsCollector(statsCollector),
		s3a.WithDiscoverBucketRegion(params.DiscoverBucketRegion),
		s3a.WithPreSignedExpiry(params.PreSignedExpiry),
		s3a.WithDisablePreSigned(params.DisablePreSigned),
		s3a.WithDisablePreSignedUI(params.DisablePreSignedUI),
		s3a.WithDisablePreSignedMultipart(params.DisablePreSignedMultipart),
	}
	if params.ServerSideEncryption != "" {
		opts = append(opts, s3a.WithServerSideEncryption(params.ServerSideEncryption))
	}
	if params.ServerSideEncryptionKmsKeyID != "" {
		opts = append(opts, s3a.WithServerSideEncryptionKmsKeyID(params.ServerSideEncryptionKmsKeyID))
	}
	if params.PreSignedEndpoint != "" {
		opts = append(opts, s3a.WithPreSignedEndpoint(params.PreSignedEndpoint))
	}
	opts = append(opts, adapterOpts...)

	adapter, err := s3a.NewAdapter(ctx, params, opts...)
	if err != nil {
		return nil, err
	}
	logging.FromContext(ctx).WithField("type", "s3").Info("initialized blockstore adapter")
	return adapter, nil
}

func BuildGSClient(ctx context.Context, params params.GS) (*storage.Client, error) {
	var opts []option.ClientOption
	if params.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(params.CredentialsFile))
	} else if params.CredentialsJSON != "" {
		cred, err := google.CredentialsFromJSON(ctx, []byte(params.CredentialsJSON), googleAuthCloudPlatform)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentials(cred))
	}
	return storage.NewClient(ctx, opts...)
}

func buildGSAdapter(ctx context.Context, params params.GS, adapterOpts ...gs.AdapterOption) (*gs.Adapter, error) {
	client, err := BuildGSClient(ctx, params)
	if err != nil {
		return nil, err
	}
	opts := []gs.AdapterOption{
		gs.WithPreSignedExpiry(params.PreSignedExpiry),
		gs.WithDisablePreSigned(params.DisablePreSigned),
		gs.WithDisablePreSignedUI(params.DisablePreSignedUI),
	}
	switch {
	case params.ServerSideEncryptionCustomerSupplied != nil:
		opts = append(opts, gs.WithServerSideEncryptionCustomerSupplied(params.ServerSideEncryptionCustomerSupplied))
	case params.ServerSideEncryptionKmsKeyID != "":
		opts = append(opts, gs.WithServerSideEncryptionKmsKeyID(params.ServerSideEncryptionKmsKeyID))
	}
	opts = append(opts, adapterOpts...)

	adapter := gs.NewAdapter(client, opts...)
	logging.FromContext(ctx).WithField("type", "gs").Info("initialized blockstore adapter")
	return adapter, nil
}
