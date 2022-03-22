package store

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func GetS3Client() (*s3.S3, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
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
		continuation = result.NextContinuationToken
	}
	return nil
}
