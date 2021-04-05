// Package helpers provides useful wrappers for clients using the lakeFS OpenAPI.
package helpers

import (
	"github.com/treeverse/lakefs/pkg/api"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ClientUpload uploads contents as a file using client-side ("direct") access to underlying
// storage.  It requires credentials both to lakeFS and to underlying storage, but
// considerably reduces the load on the lakeFS server.
func ClientUpload(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, filePath string, metadata map[string]string, contents io.ReadSeeker) (*api.ObjectStats, error) {
	resp, err := client.GetPhysicalAddressWithResponse(ctx, repoID, branchID, &api.GetPhysicalAddressParams{
		Path: filePath,
	})
	if err != nil {
		return nil, fmt.Errorf("get physical address to upload object: %w", err)
	}
	if resp.JSONDefault != nil {
		return nil, fmt.Errorf("%w: %s", ErrRequestFailed, resp.JSONDefault.Message)
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("%w: %s (status code %d)", ErrRequestFailed, resp.Status(), resp.StatusCode())
	}

	stagingLocation := *resp.JSON200
	for { // Return from inside loop
		physicalAddress := api.StringValue(stagingLocation.PhysicalAddress)
		parsedAddress, err := url.Parse(physicalAddress)
		if err != nil {
			return nil, fmt.Errorf("parse physical address URL %s: %w", physicalAddress, err)
		}

		// TODO(ariels): plug-in support for other protocols
		if parsedAddress.Scheme != "s3" {
			return nil, fmt.Errorf("%w %s", ErrUnsupportedProtocol, parsedAddress.Scheme)
		}

		bucket := parsedAddress.Hostname()
		sess, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, fmt.Errorf("connect to S3 session: %w", err)
		}
		sess.ClientConfig(s3.ServiceName)
		svc := s3.New(sess)

		// TODO(ariels): Allow customization of request
		putObjectResponse, err := svc.PutObject(&s3.PutObjectInput{
			Body:   contents,
			Bucket: &bucket,
			Key:    &parsedAddress.Path,
		})
		if err != nil {
			return nil, fmt.Errorf("upload to backing store %v: %w", parsedAddress, err)
		}

		size, err := contents.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("read size: %w", err)
		}
		_, err = contents.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("rewind: %w", err)
		}

		resp, err := client.LinkPhysicalAddressWithResponse(ctx, repoID, branchID, &api.LinkPhysicalAddressParams{
			Path: filePath,
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  api.StringValue(putObjectResponse.ETag),
			SizeBytes: size,
			Staging:   stagingLocation,
			UserMetadata: &api.StagingMetadata_UserMetadata{
				AdditionalProperties: metadata,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("link object to backing store: %w", err)
		}
		if resp.StatusCode() == http.StatusOK {
			return &api.ObjectStats{
				Checksum: *putObjectResponse.ETag,
				// BUG(ariels): Unavailable on S3, remove this field entirely
				//     OR add it to the server staging manager API.
				Mtime:           time.Now().Unix(),
				Path:            filePath,
				PathType:        "object",
				PhysicalAddress: physicalAddress,
				SizeBytes:       &size,
				UserMetadata:    &api.ObjectStats_UserMetadata{
					AdditionalProperties: metadata,
				},
			}, nil
		}
		if resp.JSON409 == nil {
			return nil, fmt.Errorf("link object to backing store: %w (status code %d)", ErrRequestFailed, resp.StatusCode())
		}
		// Try again!
		stagingLocation = *resp.JSON409
	}
}
