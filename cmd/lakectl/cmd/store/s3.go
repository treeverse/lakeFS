package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func GetS3Client(intgSrc string) (*s3.S3, error) {
	var sess *session.Session
	var err error
	if intgSrc == "minio" {
		s3EndpointUrl := os.Getenv("LAKECTL_S3_ENDPOINT_URL")
		if len(s3EndpointUrl) == 0 {
			return nil, fmt.Errorf("LAKECTL_S3_ENDPOINT_URL environment variable is not set")
		}

		sess, err = session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				Endpoint:         aws.String(s3EndpointUrl),
				Region:           aws.String("us-east-1"), // Needs region for validation as it is AWS client
				S3ForcePathStyle: aws.Bool(true),
			},
			SharedConfigState: session.SharedConfigEnable,
		})
	} else {
		sess, err = session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
	}
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	return svc, nil

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
