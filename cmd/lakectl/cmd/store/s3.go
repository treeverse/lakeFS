package store

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func GetS3Client(intg string) (*s3.S3, error) {
	if intg == "minio" {
		// Configure to use MinIO Server
		accountName, accessKey, secretKey := os.Getenv("MINIO_STORAGE_ACCOUNT"), os.Getenv("MINIO_STORAGE_ACCESS_KEY"), os.Getenv("MINIO_STORAGE_SECRET_ACCESS_KEY")
		if len(accountName) == 0 || len(accessKey) == 0 || len(secretKey) == 0 {
			return nil, fmt.Errorf("either the MINIO_STORAGE_ACCOUNT or MINIO_STORAGE_ACCESS_KEY environment variable is not set")
		}

		s3MinioConfig := &aws.Config{
			Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
			Endpoint:         aws.String(accountName),
			Region:           aws.String("us-east-1"),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		}
		sess, err := session.NewSession(s3MinioConfig)
		if err != nil {
			return nil, err
		}
		svc := s3.New(sess)
		return svc, nil
	} else {
		sess, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, err
		}
		svc := s3.New(sess)
		return svc, nil
	}

}

type S3Walker struct {
	s3 s3iface.S3API
}

func (s *S3Walker) Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error {
	var continuation *string
	const maxKeys = 1000
	prefix := strings.TrimLeft(storageURI.Path, "/")
	bucket := storageURI.Host
	for {
		result, err := s.s3.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuation,
			MaxKeys:           aws.Int64(maxKeys),
			Prefix:            aws.String(prefix),
		})
		if err != nil {
			return err
		}
		for _, record := range result.Contents {
			key := aws.StringValue(record.Key)
			addr := fmt.Sprintf("s3://%s/%s", bucket, key)
			ent := ObjectStoreEntry{
				FullKey:     key,
				RelativeKey: strings.TrimPrefix(key, prefix),
				Address:     addr,
				ETag:        strings.Trim(aws.StringValue(record.ETag), "\""),
				Mtime:       aws.TimeValue(record.LastModified),
				Size:        aws.Int64Value(record.Size),
			}
			err := walkFn(ent)
			if err != nil {
				return err
			}
		}
		if !aws.BoolValue(result.IsTruncated) {
			break
		}
		continuation = result.ContinuationToken
	}
	return nil
}
