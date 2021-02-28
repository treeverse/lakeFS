package params

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

type S3 struct {
	AwsConfig             *aws.Config
	StreamingChunkSize    int
	StreamingChunkTimeout time.Duration
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
