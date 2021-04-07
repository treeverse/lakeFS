// Package helpers provides useful wrappers for clients using the lakeFS OpenAPI.
package helpers

import (
	"github.com/treeverse/lakefs/pkg/api"

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

		adapter, err := NewAdapter(parsedAddress.Scheme)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", parsedAddress.Scheme, err)
		}

		stats, err := adapter.Upload(ctx, parsedAddress, contents)
		if err != nil {
			return nil, fmt.Errorf("upload to backing store: %w", err)
		}

		resp, err := client.LinkPhysicalAddressWithResponse(ctx, repoID, branchID, &api.LinkPhysicalAddressParams{
			Path: filePath,
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  stats.ETag,
			SizeBytes: stats.Size,
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
				Checksum: stats.ETag,
				// BUG(ariels): Unavailable on S3, remove this field entirely
				//     OR add it to the server staging manager API.
				Mtime:           time.Now().Unix(),
				Path:            filePath,
				PathType:        "object",
				PhysicalAddress: physicalAddress,
				SizeBytes:       &stats.Size,
			}, nil
		}
		if resp.JSON409 == nil {
			return nil, fmt.Errorf("link object to backing store: %w (status code %d)", ErrRequestFailed, resp.StatusCode())
		}
		// Try again!
		stagingLocation = *resp.JSON409
		if _, err = contents.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind: %w", err)
		}
	}
}
