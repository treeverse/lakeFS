package helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-openapi/swag"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	MinDownloadPartSize        int64 = 1024 * 64       // 64KB
	DefaultDownloadPartSize    int64 = 1024 * 1024 * 8 // 8MB
	DefaultDownloadConcurrency       = 10
)

type Downloader struct {
	Client         *apigen.ClientWithResponses
	PreSign        bool
	HTTPClient     *http.Client
	PartSize       int64
	ProgressWriter progress.Writer
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

	// setup progress writer
	pw := progress.NewWriter()
	pw.SetAutoStop(false)
	pw.SetSortBy(progress.SortByValue)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Options.PercentFormat = "%4.1f%%"

	return &Downloader{
		Client:         client,
		PreSign:        preSign,
		HTTPClient:     httpClient,
		PartSize:       DefaultDownloadPartSize,
		ProgressWriter: pw,
	}
}

// Download downloads an object from lakeFS to a local file, create the destination directory if needed.
func (d *Downloader) Download(ctx context.Context, src uri.URI, dst string) error {
	// create destination dir if needed
	dir := filepath.Dir(dst)
	_ = os.MkdirAll(dir, os.ModePerm)

	var err error

	// progress tracker
	tracker := &progress.Tracker{
		Message: "download " + dst,
		Total:   -1,
	}

	d.ProgressWriter.AppendTracker(tracker)
	tracker.Start()
	defer func() {
		if err != nil {
			tracker.MarkAsErrored()
		} else {
			tracker.MarkAsDone()
		}
	}()

	// download object
	if d.PreSign {
		// download using presigned multipart download, it will fall back to presign single object download if needed
		err = d.downloadPresignMultipart(ctx, src, dst, tracker)
	} else {
		err = d.downloadObject(ctx, src, dst, tracker)
	}
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	return nil
}

func (d *Downloader) downloadPresignMultipart(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker) (err error) {
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
		return d.downloadObject(ctx, src, dst, tracker)
	}

	// check if the object is small enough to download in one request
	size := swag.Int64Value(statResp.JSON200.SizeBytes)
	tracker.UpdateTotal(size)
	if size < d.PartSize {
		return d.downloadObject(ctx, src, dst, tracker)
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	// make sure the destination file is in the right size
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
			buf := make([]byte, d.PartSize)
			for part := range ch {
				err := d.downloadPresignedPart(grpCtx, physicalAddress, part.RangeStart, part.PartSize, part.Number, f, buf)
				if err != nil {
					return err
				}
				tracker.Increment(part.PartSize)
			}
			return nil
		})
	}

	// send parts to download to the channel
	partNumber := 0
	for off := int64(0); off < size; off += d.PartSize {
		partNumber++ // part numbers start from 1
		part := downloadPart{
			Number:     partNumber,
			RangeStart: off,
			PartSize:   d.PartSize,
		}
		// adjust last part size
		if part.RangeStart+part.PartSize > size {
			part.PartSize = size - part.RangeStart
		}
		ch <- part
	}
	close(ch)

	return g.Wait()
}

func (d *Downloader) downloadPresignedPart(ctx context.Context, physicalAddress string, rangeStart int64, partSize int64, partNumber int, f *os.File, buf []byte) error {
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

	// reuse buffer if possible
	if buf == nil {
		buf = make([]byte, partSize)
	} else {
		buf = buf[:partSize]
	}

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

func (d *Downloader) downloadObject(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker) error {
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

	if resp.ContentLength != -1 {
		tracker.UpdateTotal(resp.ContentLength)
	}

	// create and copy object content
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := NewTrackerWriter(f, tracker)
	_, err = io.Copy(w, resp.Body)
	return err
}

// ProgressRender start render progress and return callback waiting for the progress to finish.
func (d *Downloader) ProgressRender() func() {
	go d.ProgressWriter.Render()
	return func() {
		for d.ProgressWriter.IsRenderInProgress() {
			// for manual-stop mode, stop when there are no more active trackers
			if d.ProgressWriter.LengthActive() == 0 {
				d.ProgressWriter.Stop()
			}
			const waitForRender = 100 * time.Millisecond
			time.Sleep(waitForRender)
		}
	}
}

// Tracker interface for tracking written data.
type Tracker interface {
	Increment(int64)
}

// TrackerWriter implements io.Writer and updates a Tracker.
type TrackerWriter struct {
	w       io.Writer
	tracker Tracker
}

// NewTrackerWriter creates a new TrackerWriter.
func NewTrackerWriter(w io.Writer, tracker Tracker) *TrackerWriter {
	return &TrackerWriter{
		w:       w,
		tracker: tracker,
	}
}

func (cw *TrackerWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		cw.tracker.Increment(int64(n))
	}
	return n, err
}
