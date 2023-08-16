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
	ImportEnabled           bool
	ImportHidden            bool
	AllowedExternalPrefixes []string
}

// S3WebIdentity contains parameters for customizing S3 web identity.  This
// is also used when configuring S3 with IRSA in EKS (Kubernetes).
type S3WebIdentity struct {
	// SessionDuration is the duration WebIdentityRoleProvider will
	// request for a token for its assumed role.  It can be 1 hour or
	// more, but its maximum is configurable on AWS.
	SessionDuration time.Duration

	// SessionExpiryWindow is the time before credentials expiry that
	// the WebIdentityRoleProvider may request a fresh token.
	SessionExpiryWindow time.Duration
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
	DisablePreSigned              bool
	DisablePreSignedUI            bool
	WebIdentity                   *S3WebIdentity
}

type GS struct {
	CredentialsFile    string
	CredentialsJSON    string
	PreSignedExpiry    time.Duration
	DisablePreSigned   bool
	DisablePreSignedUI bool
}

type Azure struct {
	StorageAccount     string
	StorageAccessKey   string
	TryTimeout         time.Duration
	PreSignedExpiry    time.Duration
	DisablePreSigned   bool
	DisablePreSignedUI bool
	// TestEndpointURL - For testing purposes, provide a custom URL to override the default URL template
	TestEndpointURL string
}
