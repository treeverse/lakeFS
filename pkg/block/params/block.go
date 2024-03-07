package params

import (
	"time"
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

type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type S3 struct {
	Region                        string
	Profile                       string
	CredentialsFile               string
	Credentials                   S3Credentials
	MaxRetries                    int
	Endpoint                      string
	ForcePathStyle                bool
	DiscoverBucketRegion          bool
	SkipVerifyCertificateTestOnly bool
	ServerSideEncryption          string
	ServerSideEncryptionKmsKeyID  string
	PreSignedExpiry               time.Duration
	DisablePreSigned              bool
	DisablePreSignedUI            bool
	DisablePreSignedMultipart     bool
	ClientLogRetries              bool
	ClientLogRequest              bool
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
	// Azure China Cloud has different endpoints, different services and it's isolated from Azure Global Cloud
	ChinaCloud bool
	// TestEndpointURL - For testing purposes, provide a custom URL to override the default URL template
	TestEndpointURL string
}
