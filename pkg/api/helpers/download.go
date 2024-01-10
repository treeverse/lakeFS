package helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

func Download(ctx context.Context, client *apigen.ClientWithResponses, preSign bool, src uri.URI, dst string) error {
	// create destination dir if needed
	dir := filepath.Dir(dst)
	_ = os.MkdirAll(dir, os.ModePerm)

	// download object
	if preSign {
		return downloadPresigned(ctx, client, src, dst)
	}
	return downloadObject(ctx, client, false, src, dst)
}

func downloadPresigned(ctx context.Context, client *apigen.ClientWithResponses, src uri.URI, dst string) error {
	// get object metadata
	statResp, err := client.StatObjectWithResponse(ctx, src.Repository, src.Ref, &apigen.StatObjectParams{
		Path:    *src.Path,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return err
	}
	if statResp.JSON200 == nil || statResp.JSON200.SizeBytes == nil {
		return fmt.Errorf("bad response from server")
	}

	// check if the object is small enough to download in one request
	if *statResp.JSON200.SizeBytes < DefaultUploadPartSize {
		return downloadObject(ctx, client, true, src, dst)
	}

	// check if object support ranges in bytes
	// TODO(barak): can we assume accept ranges is supported?

	// create and copy object content
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// download the file using ranges and concurrency
	httpClient := &http.Client{}
	physicalAddress := statResp.JSON200.PhysicalAddress
	size := swag.Int64Value(statResp.JSON200.SizeBytes)
	var nextPart atomic.Int64
	g, grpCtx := errgroup.WithContext(context.Background())
	for i := 0; i < DefaultUploadConcurrency; i++ {
		g.Go(func() error {
			for {
				partNumber := nextPart.Add(1)
				start := (partNumber - 1) * DefaultUploadPartSize
				if start >= size {
					return nil
				}
				partSize := DefaultUploadPartSize
				if start+partSize > size {
					partSize = size - start
				}

				if err := downloadPresignedPart(grpCtx, httpClient, physicalAddress, start, size, partSize, partNumber, f); err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

func downloadPresignedPart(ctx context.Context, httpClient *http.Client, physicalAddress string, start int64, size int64, partSize int64, partNumber int64, f *os.File) error {
	end := start + partSize - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, physicalAddress, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", rangeHeader)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status)
	}
	if resp.ContentLength != partSize {
		return fmt.Errorf("part %d expected %d bytes, got %d", partNumber, partSize, resp.ContentLength)
	}
	writer := io.NewOffsetWriter(f, start)
	_, err = io.Copy(writer, resp.Body)
	return err
}

func downloadObject(ctx context.Context, client *apigen.ClientWithResponses, preSign bool, src uri.URI, dst string) error {
	// get object content
	resp, err := client.GetObject(ctx, src.Repository, src.Ref, &apigen.GetObjectParams{
		Path:    *src.Path,
		Presign: swag.Bool(preSign),
	})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status)
	}

	// create and copy object content
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, resp.Body)
	return err
}
