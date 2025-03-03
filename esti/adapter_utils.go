package esti

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/treeverse/lakefs/pkg/api/helpers"
)

// ObjectStats metadata of an object stored on a backing store.
type ObjectStats struct {
	// Size is the number of bytes.
	Size int64
	// ETag is a unique identifier of the contents.
	ETag string
	// MTime is the time stored for last object modification. It can be returned as 0
	// from calls to ClientAdapter.Upload().
	MTime time.Time
}

// ClientAdapter abstracts operations on a backing store.
type ClientAdapter interface {
	// Upload uploads data from contents to physicalAddress and returns stored stats.
	// Returned MTime may be zero.
	Upload(ctx context.Context, physicalAddress *url.URL, contents io.ReadSeeker) (ObjectStats, error)

	// Download returns a Reader to download data from physicalAddress.  The Close method
	// of that Reader can return errors!
	Download(ctx context.Context, physicalAddress *url.URL) (io.ReadCloser, error)
}

type AdapterFactory map[string]func() (ClientAdapter, error)

// NewAdapter returns a ClientAdapter for protocol.
func NewAdapter(protocol string) (ClientAdapter, error) {
	factory, ok := adapterFactory[protocol]
	if !ok {
		return nil, helpers.ErrUnsupportedProtocol
	}
	return factory()
}

var adapterFactory = AdapterFactory{
	s3Scheme: newS3Adapter,
}

const s3Scheme = "s3"

type s3Adapter struct {
	svc *s3.Client
}

func newS3Adapter() (ClientAdapter, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("aws load default config: %w", err)
	}
	return &s3Adapter{svc: s3.NewFromConfig(cfg)}, nil
}

func (s *s3Adapter) Upload(ctx context.Context, physicalAddress *url.URL, contents io.ReadSeeker) (ObjectStats, error) {
	if physicalAddress.Scheme != s3Scheme {
		return ObjectStats{}, fmt.Errorf("%s: %w", s3Scheme, helpers.ErrUnsupportedProtocol)
	}

	key := strings.TrimPrefix(physicalAddress.Path, "/")
	bucket := physicalAddress.Hostname()
	uploader := manager.NewUploader(s.svc)
	out, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Body:   contents,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectStats{}, err
	}
	size, err := contents.Seek(0, io.SeekEnd)
	if err != nil {
		return ObjectStats{}, fmt.Errorf("read stream size: %w", err)
	}
	return ObjectStats{
		Size: size,
		ETag: aws.ToString(out.ETag),
		// S3Manager Upload does not return creation time.
	}, nil
}

func (s *s3Adapter) Download(ctx context.Context, physicalAddress *url.URL) (io.ReadCloser, error) {
	if physicalAddress.Scheme != s3Scheme {
		return nil, fmt.Errorf("%s: %w", s3Scheme, helpers.ErrUnsupportedProtocol)
	}
	// TODO(ariels): Allow customization of request
	bucket := physicalAddress.Hostname()
	key := strings.TrimPrefix(physicalAddress.Path, "/")
	getObjectResponse, err := s.svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return getObjectResponse.Body, nil
}
