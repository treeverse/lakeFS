package helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultDownloadPartSize    int64 = 1024 * 1024 * 8 // 8MB
	DefaultDownloadConcurrency       = 10
)

type Downloader struct {
	Client     *apigen.ClientWithResponses
	PreSign    bool
	HTTPClient *http.Client
}

type downloadPart struct {
	Number     int
	RangeStart int64
	PartSize   int64
}

func NewDownloader(client *apigen.ClientWithResponses, preSign bool) *Downloader {
	// setup http client
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 10
	httpClient := &http.Client{
		Transport: transport,
	}

	return &Downloader{
		Client:     client,
		PreSign:    preSign,
		HTTPClient: httpClient,
	}
}

// Download downloads an object from lakeFS to a local file, create the destination directory if needed.
func (d *Downloader) Download(ctx context.Context, src uri.URI, dst string) error {
	// create destination dir if needed
	dir := filepath.Dir(dst)
	_ = os.MkdirAll(dir, os.ModePerm)

	// download object
	var err error
	if d.PreSign {
		// download using presigned multipart download, it will fall back to presign single object download if needed
		err = d.downloadPresignMultipart(ctx, src, dst)
	} else {
		err = d.downloadObject(ctx, src, dst)
	}
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	return nil
}

func (d *Downloader) downloadPresignMultipart(ctx context.Context, src uri.URI, dst string) (err error) {
	// get object metadata for size and physical address (presigned)
	statResp, err := d.Client.StatObjectWithResponse(ctx, src.Repository, src.Ref, &apigen.StatObjectParams{
		Path:    *src.Path,
		Presign: swag.Bool(true),
	})
	if err != nil {
		return err
	}

	// fallback to download if missing size
	if statResp.JSON200 == nil || statResp.JSON200.SizeBytes == nil {
		return d.downloadObject(ctx, src, dst)
	}

	// check if the object is small enough to download in one request
	if *statResp.JSON200.SizeBytes < DefaultDownloadPartSize {
		return d.downloadObject(ctx, src, dst)
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	// make sure the destination file is in the right size
	size := swag.Int64Value(statResp.JSON200.SizeBytes)
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("failed to truncate '%s' to size %d: %w", f.Name(), size, err)
	}

	// download the file using ranges and concurrency
	physicalAddress := statResp.JSON200.PhysicalAddress

	ch := make(chan downloadPart, DefaultDownloadConcurrency)

	// start download workers
	g, grpCtx := errgroup.WithContext(context.Background())
	for i := 0; i < DefaultDownloadConcurrency; i++ {
		g.Go(func() error {
			for part := range ch {
				err := d.downloadPresignedPart(grpCtx, physicalAddress, part.RangeStart, part.PartSize, part.Number, f)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// send parts to download to the channel
	partNumber := 0
	for off := int64(0); off < size; off += DefaultDownloadPartSize {
		partNumber++ // part numbers start from 1
		part := downloadPart{
			Number:     partNumber,
			RangeStart: off,
			PartSize:   DefaultDownloadPartSize,
		}
		if part.RangeStart+part.PartSize > size {
			part.PartSize = size - part.RangeStart
		}
		ch <- part
	}
	close(ch)

	return g.Wait()
}

func (d *Downloader) downloadPresignedPart(ctx context.Context, physicalAddress string, rangeStart int64, partSize int64, partNumber int, f *os.File) error {
	rangeEnd := rangeStart + partSize - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, physicalAddress, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", rangeHeader)
	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status)
	}
	if resp.ContentLength != partSize {
		return fmt.Errorf("%w: part %d expected %d bytes, got %d", ErrRequestFailed, partNumber, partSize, resp.ContentLength)
	}

	buf := make([]byte, partSize)
	_, err = io.ReadFull(resp.Body, buf)
	if err != nil {
		return err
	}

	_, err = f.WriteAt(buf, rangeStart)
	if err != nil {
		return err
	}
	return nil
}

func (d *Downloader) downloadObject(ctx context.Context, src uri.URI, dst string) error {
	// get object content
	resp, err := d.Client.GetObject(ctx, src.Repository, src.Ref, &apigen.GetObjectParams{
		Path:    *src.Path,
		Presign: swag.Bool(d.PreSign),
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
