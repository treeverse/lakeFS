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
	Path                    string
	AllowedExternalPrefixes []string
}

type S3 struct {
	AwsConfig                     *aws.Config
	StreamingChunkSize            int
	StreamingChunkTimeout         time.Duration
	DiscoverBucketRegion          bool
	SkipVerifyCertificateTestOnly bool
	ServerSideEncryption          string
	ServerSideEncryptionKmsKeyID  string
	PreSignedExpiry               time.Duration
}

type GS struct {
	CredentialsFile string
	CredentialsJSON string
	PreSignedExpiry time.Duration
}

type Azure struct {
	StorageAccount   string
	StorageAccessKey string
	TryTimeout       time.Duration
	PreSignedExpiry  time.Duration
}
