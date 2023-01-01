package params

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

// AdapterConfig configures a block adapter.
type AdapterConfig interface {
	BlockstoreType() string
	BlockstoreLocalParams() (Local, error)
	BlockstoreS3Params() (S3, error)
	BlockstoreGSParams() (GS, error)
	BlockstoreAzureParams() (Azure, error)
}

type Mem struct{}

type Local struct {
	Path string
}

type S3 struct {
	AwsConfig                     *aws.Config
	StreamingChunkSize            int
	StreamingChunkTimeout         time.Duration
	DiscoverBucketRegion          bool
	SkipVerifyCertificateTestOnly bool
	ServerSideEncryption          string
	ServerSideEncryptionKmsKeyID  string
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
