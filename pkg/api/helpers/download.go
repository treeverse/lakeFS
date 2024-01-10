package helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultDownloadPartSize    int64 = 1024 * 1024 * 8 // 8MB
	DefaultDownloadConcurrency       = 10
)

func Download(ctx context.Context, client *apigen.ClientWithResponses, preSign bool, src uri.URI, dst string) error {
	start := time.Now()
	defer func() {
		fmt.Printf("Downloaded %s in %s\n", src.String(), time.Since(start))
	}()
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
		return fmt.Errorf("%w: missing object size (%s)", ErrRequestFailed, statResp.Status())
	}

	// check if the object is small enough to download in one request
	if *statResp.JSON200.SizeBytes < DefaultDownloadPartSize {
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

	// make sure the file is the right size
	size := swag.Int64Value(statResp.JSON200.SizeBytes)
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("failed to truncate file to size %d: %w", size, err)
	}

	// download the file using ranges and concurrency
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 10
	httpClient := &http.Client{
		Transport: transport,
	}

	physicalAddress := statResp.JSON200.PhysicalAddress

	var nextPart atomic.Int64
	g, grpCtx := errgroup.WithContext(context.Background())
	for i := 0; i < DefaultDownloadConcurrency; i++ {
		g.Go(func() error {
			for {
				partNumber := nextPart.Add(1)
				rangeStart := (partNumber - 1) * DefaultDownloadPartSize
				if rangeStart >= size {
					return nil
				}
				partSize := DefaultDownloadPartSize
				if rangeStart+partSize > size {
					partSize = size - rangeStart
				}

				err := downloadPresignedPart(grpCtx, httpClient, physicalAddress, rangeStart, partSize, partNumber, f)
				if err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

func downloadPresignedPart(ctx context.Context, httpClient *http.Client, physicalAddress string, rangeStart int64, partSize int64, partNumber int64, f *os.File) error {
	// fmt.Printf("Downloading part %d (%d)...\n", partNumber, partSize)
	start := time.Now()
	defer func() {
		fmt.Printf("Downloaded part %d (%d), took %s.\n", partNumber, partSize, time.Since(start))
	}()

	rangeEnd := rangeStart + partSize - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, physicalAddress, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", rangeHeader)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status)
	}
	if resp.ContentLength != partSize {
		return fmt.Errorf("part %d expected %d bytes, got %d", partNumber, partSize, resp.ContentLength)
	}
	writer := io.NewOffsetWriter(f, rangeStart)
	_, err = io.CopyN(writer, resp.Body, partSize)
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
