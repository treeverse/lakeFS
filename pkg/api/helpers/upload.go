// Package helpers provides useful wrappers for clients using the lakeFS OpenAPI.
package helpers

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
)

// ClientUpload uploads contents as a file using client-side ("direct") access to underlying
// storage.  It requires credentials both to lakeFS and to underlying storage, but
// considerably reduces the load on the lakeFS server.
func ClientUpload(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*api.ObjectStats, error) {
	stagingLocation, err := getPhysicalAddress(ctx, client, repoID, branchID, &api.GetPhysicalAddressParams{
		Path:    objPath,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return nil, err
	}

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
			Path: objPath,
		}, api.LinkPhysicalAddressJSONRequestBody{
			Checksum:  stats.ETag,
			SizeBytes: stats.Size,
			Staging:   *stagingLocation,
			UserMetadata: &api.StagingMetadata_UserMetadata{
				AdditionalProperties: metadata,
			},
			ContentType: &contentType,
		})
		if err != nil {
			return nil, fmt.Errorf("link object to backing store: %w", err)
		}
		if resp.JSON200 != nil {
			return resp.JSON200, nil
		}
		if resp.JSON409 == nil {
			return nil, fmt.Errorf("link object to backing store: %w (status code %d)", ErrRequestFailed, resp.StatusCode())
		}
		// Try again!
		if _, err = contents.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind: %w", err)
		}
	}
}

func ClientUploadPreSign(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*api.ObjectStats, error) {
	stagingLocation, err := getPhysicalAddress(ctx, client, repoID, branchID, &api.GetPhysicalAddressParams{
		Path:    objPath,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	// TODO(barak): upload content using pre-sign URL
	preSignURL := swag.StringValue(stagingLocation.PresignedUrl)
	_ = preSignURL
	return nil
}

func getPhysicalAddress(ctx context.Context, client api.ClientWithResponsesInterface, repoID string, branchID string, params *api.GetPhysicalAddressParams) (*api.StagingLocation, error) {
	resp, err := client.GetPhysicalAddressWithResponse(ctx, repoID, branchID, params)
	if err != nil {
		return nil, fmt.Errorf("get physical address to upload object: %w", err)
	}
	if resp.JSONDefault != nil {
		return nil, fmt.Errorf("%w: %s", ErrRequestFailed, resp.JSONDefault.Message)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("%w: %s (status code %d)", ErrRequestFailed, resp.Status(), resp.StatusCode())
	}
	return resp.JSON200, nil
}
