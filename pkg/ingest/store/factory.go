package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/block/gs"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/block/s3"
)

var ErrNotSupported = errors.New("no storage adapter found")

type WalkerOptions struct {
	S3EndpointURL string
	StorageURI    string
}

type WalkerWrapper struct {
	walker block.Walker
	uri    *url.URL
}

func NewWrapper(walker block.Walker, uri *url.URL) *WalkerWrapper {
	return &WalkerWrapper{
		walker: walker,
		uri:    uri,
	}
}

func (ww *WalkerWrapper) Walk(ctx context.Context, opts block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	return ww.walker.Walk(ctx, ww.uri, opts, walkFn)
}

func (ww *WalkerWrapper) Marker() block.Mark {
	return ww.walker.Marker()
}

func (ww *WalkerWrapper) GetSkippedEntries() []block.ObjectStoreEntry {
	return ww.walker.GetSkippedEntries()
}

type WalkerFactory struct {
	params params.AdapterConfig
}

func NewFactory(params params.AdapterConfig) *WalkerFactory {
	return &WalkerFactory{params: params}
}

func (f *WalkerFactory) buildS3Walker(opts WalkerOptions) (*s3.Walker, error) {
	var sess *session.Session
	if f.params != nil {
		s3params, err := f.params.BlockstoreS3Params()
		if err != nil {
			return nil, err
		}
		sess, err = factory.BuildS3Client(s3params.AwsConfig, s3params.SkipVerifyCertificateTestOnly)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		sess, err = getS3Client(opts.S3EndpointURL)
		if err != nil {
			return nil, err
		}
	}
	return s3.NewS3Walker(sess), nil
}

func (f *WalkerFactory) buildGCSWalker(ctx context.Context) (*gs.GCSWalker, error) {
	var svc *storage.Client
	if f.params != nil {
		gsParams, err := f.params.BlockstoreGSParams()
		if err != nil {
			return nil, err
		}
		svc, err = factory.BuildGSClient(ctx, gsParams)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		svc, err = storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
	}
	return gs.NewGCSWalker(svc), nil
}

func (f *WalkerFactory) buildAzureWalker(importURL *url.URL) (block.Walker, error) {
	storageAccount, err := azure.ExtractStorageAccount(importURL)
	if err != nil {
		return nil, err
	}

	var azureParams params.Azure
	if f.params != nil {
		// server settings
		azureParams, err = f.params.BlockstoreAzureParams()
		if err != nil {
			return nil, err
		}
	}

	// Use StorageAccessKey to initialize storage account client only if it was provided for this given storage account
	// Otherwise fall back to the default credentials
	if azureParams.StorageAccount != storageAccount {
		azureParams.StorageAccount = storageAccount
		azureParams.StorageAccessKey = ""
	}
	client, err := azure.BuildAzureServiceClient(azureParams)
	if err != nil {
		return nil, err
	}

	isHNS := isHierarchicalNamespaceEnabled(importURL)
	if isHNS {
		return azure.NewAzureDataLakeWalker(client)
	}
	return azure.NewAzureBlobWalker(client)
}

// isHierarchicalNamespaceEnabled - identify if hns enabled on the account,
// based on the import URL.
// Until we enable a way to extract the account information, we assume it based on the domain used in import:
// https://<account>.<blob|adls>.core.windows.net/
// adls - azure data lake storage
func isHierarchicalNamespaceEnabled(u *url.URL) bool {
	const importURLParts = 3
	n := strings.SplitN(u.Host, ".", importURLParts)
	return len(n) == importURLParts && n[1] == "adls"
}

func (f *WalkerFactory) GetWalker(ctx context.Context, opts WalkerOptions) (*WalkerWrapper, error) {
	uri, err := url.Parse(opts.StorageURI)
	if err != nil {
		return nil, fmt.Errorf("could not parse storage URI %s: %w", uri, err)
	}

	var walker block.Walker
	switch uri.Scheme {
	case "s3":
		walker, err = f.buildS3Walker(opts)
		if err != nil {
			return nil, fmt.Errorf("creating s3 walker: %w", err)
		}
	case "gs":
		walker, err = f.buildGCSWalker(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating gs walker: %w", err)
		}
	case "http", "https":
		walker, err = f.buildAzureWalker(uri)
		if err != nil {
			return nil, fmt.Errorf("creating Azure walker: %w", err)
		}
	case "local":
		walker, err = f.buildLocalWalker()
		if err != nil {
			return nil, fmt.Errorf("creating local walker: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w: for scheme: %s", ErrNotSupported, uri.Scheme)
	}
	return NewWrapper(walker, uri), nil
}

func (f *WalkerFactory) buildLocalWalker() (*local.Walker, error) {
	var (
		localParams params.Local
		err         error
	)

	if f.params != nil {
		localParams, err = f.params.BlockstoreLocalParams()
		if err != nil {
			return nil, err
		}
	}

	return local.NewLocalWalker(localParams), nil
}

func getS3Client(s3EndpointURL string) (*session.Session, error) {
	var config aws.Config
	if s3EndpointURL != "" {
		config = aws.Config{
			Endpoint:         aws.String(s3EndpointURL),
			Region:           aws.String("us-east-1"), // Needs region for validation as it is AWS client
			S3ForcePathStyle: aws.Bool(true),
		}
	}
	return session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            config,
	})
}
