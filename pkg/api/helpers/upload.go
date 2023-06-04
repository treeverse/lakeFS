// Package helpers provides useful wrappers for clients using the lakeFS OpenAPI.
package helpers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
)

// ClientUploadDirect uploads contents as a file using client-side ("direct") access to underlying
// storage.  It requires credentials both to lakeFS and to underlying storage, but
// considerably reduces the load on the lakeFS server.
func ClientUploadDirect(ctx context.Context, client api.ClientWithResponsesInterface, repoID, branchID, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker) (*api.ObjectStats, error) {
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
	// calculate size using seek
	contentLength, err := contents.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// upload loop, retry on conflict
	for {
		stats, err := clientUploadPreSignHelper(ctx, client, repoID, branchID, objPath, metadata, contentType, contents, contentLength)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return stats, nil
	}
}

// clientUploadPreSignHelper helper func to get physical address an upload content. Special case if conflict that
// ErrConflict is returned where a re-try is required.
func clientUploadPreSignHelper(ctx context.Context, client api.ClientWithResponsesInterface, repoID string, branchID string, objPath string, metadata map[string]string, contentType string, contents io.ReadSeeker, contentLength int64) (*api.ObjectStats, error) {
	stagingLocation, err := getPhysicalAddress(ctx, client, repoID, branchID, &api.GetPhysicalAddressParams{
		Path:    objPath,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	preSignURL := swag.StringValue(stagingLocation.PresignedUrl)
	if _, err := contents.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if contentLength == 0 {
		// If we pass a reader to http.NewRequestWithContext, a Transfer-Encoding header will be added as well.
		// In cases where body is empty, S3 would fail that request.
		contents = nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, preSignURL, contents)
	if err != nil {
		return nil, err
	}
	req.ContentLength = contentLength
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upload %w %s: %s", ErrRequestFailed, preSignURL, putResp.Status)
	}

	etag := putResp.Header.Get("Etag")
	etag = strings.TrimSpace(etag)
	if etag == "" {
		return nil, fmt.Errorf("etag is missing: %w", ErrRequestFailed)
	}

	linkReqBody := api.LinkPhysicalAddressJSONRequestBody{
		Checksum:  etag,
		SizeBytes: contentLength,
		Staging:   *stagingLocation,
		UserMetadata: &api.StagingMetadata_UserMetadata{
			AdditionalProperties: metadata,
		},
		ContentType: api.StringPtr(contentType),
	}
	linkResp, err := client.LinkPhysicalAddressWithResponse(ctx, repoID, branchID,
		&api.LinkPhysicalAddressParams{
			Path: objPath,
		}, linkReqBody)
	if err != nil {
		return nil, fmt.Errorf("link object to backing store: %w", err)
	}
	if linkResp.JSON200 != nil {
		return linkResp.JSON200, nil
	}
	if linkResp.JSON409 != nil {
		return nil, ErrConflict
	}
	return nil, fmt.Errorf("link object to backing store: %w (%s)", ErrRequestFailed, linkResp.Status())
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
