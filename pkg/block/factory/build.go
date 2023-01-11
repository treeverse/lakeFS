package factory

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
	"github.com/treeverse/lakefs/pkg/block/transient"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// googleAuthCloudPlatform - Cloud Storage authentication https://cloud.google.com/storage/docs/authentication
const googleAuthCloudPlatform = "https://www.googleapis.com/auth/cloud-platform"

var (
	ErrInvalidBlockStoreType  = errors.New("invalid blockstore type")
	ErrAuthMethodNotSupported = errors.New("authentication method not supported")
)

func BuildBlockAdapter(ctx context.Context, statsCollector stats.Collector, c params.AdapterConfig) (block.Adapter, error) {
	blockstore := c.BlockstoreType()
	logging.Default().
		WithField("type", blockstore).
		Info("initialize blockstore adapter")
	switch blockstore {
	case block.BlockstoreTypeLocal:
		p, err := c.BlockstoreLocalParams()
		if err != nil {
			return nil, err
		}
		return buildLocalAdapter(p)
	case block.BlockstoreTypeS3:
		p, err := c.BlockstoreS3Params()
		if err != nil {
			return nil, err
		}
		return buildS3Adapter(statsCollector, p)
	case block.BlockstoreTypeMem, "memory":
		return mem.New(), nil
	case block.BlockstoreTypeTransient:
		return transient.New(), nil
	case block.BlockstoreTypeGS:
		p, err := c.BlockstoreGSParams()
		if err != nil {
			return nil, err
		}
		return buildGSAdapter(ctx, p)
	case block.BlockstoreTypeAzure:
		p, err := c.BlockstoreAzureParams()
		if err != nil {
			return nil, err
		}
		return buildAzureAdapter(p)
	default:
		return nil, fmt.Errorf("%w '%s' please choose one of %s",
			ErrInvalidBlockStoreType, blockstore, []string{block.BlockstoreTypeLocal, block.BlockstoreTypeS3, block.BlockstoreTypeAzure, block.BlockstoreTypeMem, block.BlockstoreTypeTransient, block.BlockstoreTypeGS})
	}
}

func buildLocalAdapter(params params.Local) (*local.Adapter, error) {
	adapter, err := local.NewAdapter(params.Path)
	if err != nil {
		return nil, fmt.Errorf("got error opening a local block adapter with path %s: %w", params.Path, err)
	}
	logging.Default().WithFields(logging.Fields{
		"type": "local",
		"path": params.Path,
	}).Info("initialized blockstore adapter")
	return adapter, nil
}

func BuildS3Client(params *aws.Config, skipVerifyCertificateTestOnly bool) (*session.Session, error) {
	client := http.DefaultClient
	if skipVerifyCertificateTestOnly {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}
		client = &http.Client{Transport: tr}
	}
	sess, err := session.NewSession(params, aws.NewConfig().WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	sess.ClientConfig(s3.ServiceName)
	return sess, nil
}

func buildS3Adapter(statsCollector stats.Collector, params params.S3) (*s3a.Adapter, error) {
	sess, err := BuildS3Client(params.AwsConfig, params.SkipVerifyCertificateTestOnly)
	if err != nil {
		return nil, err
	}
	opts := []s3a.AdapterOption{
		s3a.WithStreamingChunkSize(params.StreamingChunkSize),
		s3a.WithStreamingChunkTimeout(params.StreamingChunkTimeout),
		s3a.WithStatsCollector(statsCollector),
		s3a.WithDiscoverBucketRegion(params.DiscoverBucketRegion),
	}
	if params.ServerSideEncryption != "" {
		opts = append(opts, s3a.WithServerSideEncryption(params.ServerSideEncryption))
	}
	if params.ServerSideEncryptionKmsKeyID != "" {
		opts = append(opts, s3a.WithServerSideEncryptionKmsKeyID(params.ServerSideEncryptionKmsKeyID))
	}
	adapter := s3a.NewAdapter(sess, opts...)
	logging.Default().WithField("type", "s3").Info("initialized blockstore adapter")
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

func buildGSAdapter(ctx context.Context, params params.GS) (*gs.Adapter, error) {
	client, err := BuildGSClient(ctx, params)
	if err != nil {
		return nil, err
	}
	adapter := gs.NewAdapter(client)
	logging.Default().WithField("type", "gs").Info("initialized blockstore adapter")
	return adapter, nil
}

func buildAzureAdapter(params params.Azure) (*azure.Adapter, error) {
	p, err := BuildAzureServiceClient(params)
	if err != nil {
		return nil, err
	}
	return azure.NewAdapter(p), nil
}

func BuildAzureClient(params params.Azure) (*azblob.Client, error) {
	// accountName := params.StorageAccount
	// accountKey := params.StorageAccessKey
	// var credentials *azblob.SharedKeyCredential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	// var err error
	// switch params.AuthMethod {
	// case azure.AuthMethodAccessKey:
	// 	credentials, err = azblob.NewSharedKeyCredential(accountName, accountKey)
	// // case azure.AuthMethodMSI:
	// // 	credentials, err = azure.GetMSICredentials()
	// default:
	// 	err = ErrAuthMethodNotSupported
	// }
	// if err != nil {
	// 	return nil, fmt.Errorf("invalid credentials: %w", err)
	// }
	// return azblob.NewPipeline(credentials, azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: params.TryTimeout}}), nil
	return azblob.NewClient("https://testmultigen2.blob.core.windows.net/", cred, nil)
}

func BuildAzureServiceClient(params params.Azure) (*service.Client, error) {
	// accountName := params.StorageAccount
	// accountKey := params.StorageAccessKey
	// var credentials *azblob.SharedKeyCredential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	// var err error
	// switch params.AuthMethod {
	// case azure.AuthMethodAccessKey:
	// 	credentials, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	// // case azure.AuthMethodMSI:
	//  	credentials, err := azure.GetMSICredentials()
	// default:
	// 	err = ErrAuthMethodNotSupported
	// }
	// if err != nil {
	// 	return nil, fmt.Errorf("invalid credentials: %w", err)
	// }
	// return azblob.NewPipeline(credentials, azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: params.TryTimeout}}), nil
	return service.NewClient("https://testmultigen2.blob.core.windows.net/", cred, nil)
}
