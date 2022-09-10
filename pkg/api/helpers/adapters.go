package helpers

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/treeverse/lakefs/pkg/api"
)

// ObjectStats metadata of an object stored on a backing store.
type ObjectStats struct {
	// Size is the number of bytes.
	Size int64
	// ETag is a unique identifier of the contents.
	ETag string
	// MTime is the time stored for last object modification.  It can be returned as 0
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
		return nil, ErrUnsupportedProtocol
	}
	return factory()
}

var adapterFactory = AdapterFactory{
	s3Scheme: newS3Adapter,
}

const s3Scheme = "s3"

type s3Adapter struct {
	svc  *s3.S3
	sess client.ConfigProvider
}

func newS3Adapter() (ClientAdapter, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to S3 session: %w", err)
	}
	sess.ClientConfig(s3.ServiceName)
	return &s3Adapter{sess: sess, svc: s3.New(sess)}, nil
}

func (s *s3Adapter) Upload(ctx context.Context, physicalAddress *url.URL, contents io.ReadSeeker) (ObjectStats, error) {
	if physicalAddress.Scheme != s3Scheme {
		return ObjectStats{}, fmt.Errorf("%s: %w", s3Scheme, ErrUnsupportedProtocol)
	}

	manager := s3manager.NewUploader(s.sess)
	out, err := manager.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   contents,
		Bucket: api.StringPtr(physicalAddress.Hostname()),
		Key:    &physicalAddress.Path,
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
		ETag: api.StringValue(out.ETag),
		// S3Manager Upload does not return creation time.
	}, nil
}

func (s *s3Adapter) Download(ctx context.Context, physicalAddress *url.URL) (io.ReadCloser, error) {
	if physicalAddress.Scheme != s3Scheme {
		return nil, fmt.Errorf("%s: %w", s3Scheme, ErrUnsupportedProtocol)
	}
	// TODO(ariels): Allow customization of request
	getObjectResponse, err := s.svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: api.StringPtr(physicalAddress.Hostname()),
		Key:    &physicalAddress.Path,
	})
	if err != nil {
		return nil, err
	}
	return getObjectResponse.Body, nil
}
