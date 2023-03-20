package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/pkg/block"
)

type Walker struct {
	s3   s3iface.S3API
	mark block.Mark
}

func NewS3Walker(sess *session.Session) *Walker {
	return &Walker{
		s3:   s3.New(sess),
		mark: block.Mark{HasMore: true},
	}
}

func (s *Walker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	var continuation *string
	const maxKeys = 1000
	prefix := strings.TrimLeft(storageURI.Path, "/")

	// basePath is the path relative to which the walk is done. The key of the resulting entries will be relative to this path.
	// As the original prefix might not end with a separator, it cannot be used for the
	// trim purpose, as this will create partial "folder" names. When the basePath is
	// trimmed from the key, the remains will be the object name.
	// Example:
	// Say we have the following keys:
	// pref/object
	// pref/obj/another
	// If we specify prefix="pref/obj" (both keys will be listed) then basePath="pref/" and the trim result
	// for the keys will be:
	// object
	// obj/another
	var basePath string
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		basePath = prefix[:idx+1]
	}
	bucket := storageURI.Host
	for {
		result, err := s.s3.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuation,
			MaxKeys:           aws.Int64(maxKeys),
			Prefix:            aws.String(prefix),
			StartAfter:        aws.String(op.After),
		})
		if continuation != nil {
			s.mark.ContinuationToken = *continuation
		}
		if err != nil {
			return err
		}
		for _, record := range result.Contents {
			key := aws.StringValue(record.Key)
			addr := fmt.Sprintf("s3://%s/%s", bucket, key)
			ent := block.ObjectStoreEntry{
				FullKey:     key,
				RelativeKey: strings.TrimPrefix(key, basePath),
				Address:     addr,
				ETag:        strings.Trim(aws.StringValue(record.ETag), "\""),
				Mtime:       aws.TimeValue(record.LastModified),
				Size:        aws.Int64Value(record.Size),
			}
			s.mark.LastKey = key
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
	s.mark = block.Mark{
		LastKey: "",
		HasMore: false,
	}
	return nil
}

func (s *Walker) Marker() block.Mark {
	return s.mark
}
