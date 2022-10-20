package helpers

import (
	"github.com/treeverse/lakefs/pkg/api"

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
		return nil, nil, fmt.Errorf("stat object: %w", err)
	}
	if resp.JSON200 == nil {
		return nil, nil, fmt.Errorf("stat object: %w", ErrRequestFailed)
	}
	physicalAddress := resp.JSON200.PhysicalAddress
	parsedAddress, err := url.Parse(physicalAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("parse physical address URL %s: %w", physicalAddress, err)
	}

	adapter, err := NewAdapter(parsedAddress.Scheme)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot handle %s: %w", parsedAddress.Scheme, err)
	}

	contents, err := adapter.Download(ctx, parsedAddress)

	if err != nil {
		return nil, nil, fmt.Errorf("read from backing store %v: %w", parsedAddress, err)
	}

	return resp.JSON200, contents, nil
}
