package params

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

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
}
