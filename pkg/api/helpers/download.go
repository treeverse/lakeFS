package helpers

import (
	"github.com/treeverse/lakefs/pkg/api"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"context"
	"fmt"
	"io"
	"net/url"
)

// ClientDownload downloads a file using client-side ("direct") access to underlying storage.
// It requires credentials both to lakeFS and to underlying storage, but considerably reduces
// the load on the lakeFS server.
func ClientDownload(ctx context.Context, client api.ClientWithResponsesInterface, repoID, ref, filePath string) (*api.ObjectStats, io.ReadCloser, error) {
	resp, err := client.StatObjectWithResponse(ctx, repoID, ref, &api.StatObjectParams{
		Path: filePath,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("stat object: %w", err)
	}
	if err := ResponseAsError(resp); err != nil {
		return nil, nil, fmt.Errorf("get object URI: %w", err)
	}

	physicalAddress:= resp.JSON200.PhysicalAddress
	parsedAddress, err := url.Parse(physicalAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("parse physical address URL %s: %w", physicalAddress, err)
	}

	// TODO(ariels): plug-in support for other protocols
	if parsedAddress.Scheme != "s3" {
		return nil, nil, fmt.Errorf("%w %s", ErrUnsupportedProtocol, parsedAddress.Scheme)
	}

	bucket := parsedAddress.Hostname()
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to S3 session: %w", err)
	}
	sess.ClientConfig(s3.ServiceName)
	svc := s3.New(sess)

	// TODO(ariels): Allow customization of request
	getObjectResponse, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &parsedAddress.Path,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read from backing store %v: %w", parsedAddress, err)
	}

	return resp.JSON200, getObjectResponse.Body, nil
}
