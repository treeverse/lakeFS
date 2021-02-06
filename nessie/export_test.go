package nessie

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func NewExternalS3Service() *s3.S3 {
	awsSession := session.Must(session.NewSession())
	return s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     viper.GetString("aws_access_key_id"),
						SecretAccessKey: viper.GetString("aws_secret_access_key"),
					}})))

}

func parsePath(t testing.TB, path string) (string, string) {
	t.Helper()
	u, err := url.Parse(path)
	require.NoError(t, err, "failed to parse path")
	bucket := u.Host
	keyPath := strings.TrimLeft(u.Path, "/")
	return bucket, keyPath
}

func s3GetObjectRetry(s3Svc *s3.S3, bucket string, key string) (*s3.GetObjectOutput, error) {
	var err error
	var objectOutput *s3.GetObjectOutput
	count := 0
	for range time.Tick(5 * time.Second) {
		count++
		objectOutput, err = s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil || count > 5 {
			break
		}
	}
	return objectOutput, err
}
