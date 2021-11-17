package params

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// AdapterConfig configures a block adapter.
type AdapterConfig interface {
	GetBlockstoreType() string
	GetBlockAdapterLocalParams() (Local, error)
	GetBlockAdapterS3Params() (S3, error)
	GetBlockAdapterGSParams() (GS, error)
	GetBlockAdapterAzureParams() (Azure, error)
}

type Mem struct{}

type Local struct {
	Path string
}

type AWSParams struct {
	Config      *aws.Config
	EndpointURL string
	// Region is a default region value to use if not looking bucket
	// regions up on S3, or if a lookup fails.
	//
	// TODO(ariels): Can we just set it on Config?
	Region string
	S3     struct {
		ForcePathStyle bool
	}
}

type S3 struct {
	AWSParams             AWSParams
	StreamingChunkSize    int
	StreamingChunkTimeout time.Duration
	DiscoverBucketRegion  bool
}

type GS struct {
	CredentialsFile string
	CredentialsJSON string
}

type Azure struct {
	StorageAccount   string
	StorageAccessKey string
	AuthMethod       string
	TryTimeout       time.Duration
}
