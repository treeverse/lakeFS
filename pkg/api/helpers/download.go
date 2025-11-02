package helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/swag"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	MinDownloadPartSize        int64 = 1024 * 64       // 64KB
	DefaultDownloadPartSize    int64 = 1024 * 1024 * 8 // 8MB
	DefaultDownloadConcurrency       = 10
)

// Backoff parameters for download retries
const (
	DefaultDownloadInitialInterval = 100 * time.Millisecond
	DefaultDownloadMaxInterval     = 2 * time.Second
	DefaultDownloadMaxElapsedTime  = 30 * time.Second
)

// newDownloadBackoff creates a backoff strategy for download retries.
func newDownloadBackoff() backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(DefaultDownloadInitialInterval),
		backoff.WithMaxInterval(DefaultDownloadMaxInterval),
		backoff.WithMaxElapsedTime(DefaultDownloadMaxElapsedTime),
	)
}

type Downloader struct {
	Client              *apigen.ClientWithResponses
	PreSign             bool
	HTTPClient          *http.Client
	PartSize            int64
	SkipNonRegularFiles bool
	SymlinkSupport      bool
}

type downloadPart struct {
	Number     int
	RangeStart int64
	PartSize   int64
}

func NewDownloader(client *apigen.ClientWithResponses, preSign bool) *Downloader {
	// setup http client
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 50

	retryClient := retryablehttp.NewClient()
	retryClient.Logger = &stats.LoggerAdapter{Logger: logging.ContextUnavailable().WithField("component", "downloader")}
	retryClient.RetryMax = 3
	retryClient.Backoff = retryablehttp.DefaultBackoff
	retryClient.HTTPClient.Transport = transport
	httpClient := retryClient.StandardClient()

	return &Downloader{
		Client:              client,
		PreSign:             preSign,
		HTTPClient:          httpClient,
		PartSize:            DefaultDownloadPartSize,
		SkipNonRegularFiles: false,
		SymlinkSupport:      false,
	}
}

// downloadObjectCore handles the common download logic for both Download and DownloadWithObjectInfo
func (d *Downloader) downloadObjectCore(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker, objectStat *apigen.ObjectStats) error {
	// Validate that object stats are provided when required
	if (d.SymlinkSupport || d.PreSign) && objectStat == nil {
		return fmt.Errorf("object stats are required for symlink support or presign downloads but were not provided: %w", ErrValidation)
	}

	// delete destination file if it exists
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing destination file '%s': %w", dst, err)
	}

	// create destination dir if needed
	dir := filepath.Dir(dst)
	_ = os.MkdirAll(dir, os.ModePerm)

	// If symlink support is enabled, check if the object is a symlink and create it if so
	if d.SymlinkSupport && objectStat != nil {
		symlinkTarget, found := objectStat.Metadata.Get(apiutil.SymlinkMetadataKey)
		if found && symlinkTarget != "" {
			if tracker != nil {
				tracker.UpdateTotal(0)
				tracker.MarkAsDone()
			}
			// Skip non-regular files
			if d.SkipNonRegularFiles {
				return nil
			}
			// Create symlink instead of downloading file content
			return os.Symlink(symlinkTarget, dst)
		}
		// fallthrough to download the object
	}

	// download object
	var err error
	if d.PreSign && objectStat != nil && swag.Int64Value(objectStat.SizeBytes) >= d.PartSize {
		// download using presigned multipart download, it will fall back to presign single object download if needed
		err = d.downloadPresignMultipart(ctx, src, dst, tracker, objectStat)
	} else {
		err = d.downloadObject(ctx, src, dst, tracker)
	}
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	return nil
}

// DownloadWithObjectInfo downloads an object from lakeFS using pre-fetched object information,
// avoiding the need for a separate stat call.
func (d *Downloader) DownloadWithObjectInfo(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker, objectStat *apigen.ObjectStats) error {
	return d.downloadObjectCore(ctx, src, dst, tracker, objectStat)
}

// Download downloads an object from lakeFS to a local file, create the destination directory if needed.
func (d *Downloader) Download(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker) error {
	// Check if we need to call StatObjectWithResponse (for symlinks or presign multipart)
	var objectStat *apigen.ObjectStats
	if d.SymlinkSupport || d.PreSign {
		statResp, err := d.Client.StatObjectWithResponse(ctx, src.Repository, src.Ref, &apigen.StatObjectParams{
			Path:         apiutil.Value(src.Path),
			UserMetadata: swag.Bool(d.SymlinkSupport), // Only request metadata if symlink support is enabled
			Presign:      swag.Bool(d.PreSign),        // Only presign if needed
		})
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
		if statResp.JSON200 == nil {
			return fmt.Errorf("download failed: %w: %s", ErrRequestFailed, statResp.Status())
		}
		objectStat = statResp.JSON200
	}

	return d.downloadObjectCore(ctx, src, dst, tracker, objectStat)
}

// downloadPresignMultipart downloads a large object, must be larger or equal to PartSize using a presigned URL.
// It uses multiple concurrent range requests to download the object in parts.
// If the object is smaller than PartSize, it falls back to a single `downloadObject` call.
func (d *Downloader) downloadPresignMultipart(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker, objectStat *apigen.ObjectStats) (err error) {
	// check if the object is small enough to download in one request
	size := swag.Int64Value(objectStat.SizeBytes)
	if size < d.PartSize {
		return fmt.Errorf("object is smaller than PartSize (%d): %w", d.PartSize, ErrValidation)
	}
	if tracker != nil {
		tracker.UpdateTotal(size)
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

	// download the file using ranges and concurrency with a fresh presigned URL
	statResp, err := d.Client.StatObjectWithResponse(ctx, src.Repository, src.Ref, &apigen.StatObjectParams{
		Path:         apiutil.Value(src.Path),
		UserMetadata: swag.Bool(d.SymlinkSupport), // Only request metadata if symlink support is enabled
		Presign:      swag.Bool(d.PreSign),        // Only presign if needed
	})
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	if statResp.JSON200 == nil {
		return fmt.Errorf("download failed: %w: %s", ErrRequestFailed, statResp.Status())
	}
	physicalAddress := statResp.JSON200.PhysicalAddress

	// start download workers
	ch := make(chan downloadPart, DefaultDownloadConcurrency)
	g, grpCtx := errgroup.WithContext(context.Background())
	for range DefaultDownloadConcurrency {
		g.Go(func() error {
			buf := make([]byte, d.PartSize)
			for part := range ch {
				err := d.downloadPresignedPart(grpCtx, physicalAddress, part.RangeStart, part.PartSize, part.Number, f, buf)
				if err != nil {
					return err
				}
				if tracker != nil {
					tracker.Increment(part.PartSize)
				}
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
	// set range header
	rangeEnd := rangeStart + partSize - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)

	// reuse buffer if possible
	if buf == nil {
		buf = make([]byte, partSize)
	} else {
		buf = buf[:partSize]
	}

	operation := func() error {
		// create request with range header
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, physicalAddress, nil)
		if err != nil {
			return backoff.Permanent(err)
		}
		req.Header.Set("Range", rangeHeader)

		resp, err := d.HTTPClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusPartialContent {
			return backoff.Permanent(fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status))
		}
		if resp.ContentLength != partSize {
			return backoff.Permanent(fmt.Errorf("%w: part %d expected %d bytes, got %d", ErrRequestFailed, partNumber, partSize, resp.ContentLength))
		}
		_, readErr := io.ReadFull(resp.Body, buf)
		return readErr
	}

	bo := backoff.WithContext(newDownloadBackoff(), ctx)
	if err := backoff.Retry(operation, bo); err != nil {
		return fmt.Errorf("failed to download part %d: %w", partNumber, err)
	}

	_, err := f.WriteAt(buf, rangeStart)
	return err
}

func (d *Downloader) downloadObject(ctx context.Context, src uri.URI, dst string, tracker *progress.Tracker) error {
	operation := func() error {
		// get object content
		resp, err := d.Client.GetObject(ctx, src.Repository, src.Ref, &apigen.GetObjectParams{
			Path:    *src.Path,
			Presign: swag.Bool(d.PreSign),
		})
		if err != nil {
			return backoff.Permanent(err)
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		if resp.StatusCode != http.StatusOK {
			return backoff.Permanent(fmt.Errorf("%w: %s", ErrRequestFailed, resp.Status))
		}

		// create and copy object content
		f, err := os.Create(dst)
		if err != nil {
			return backoff.Permanent(err)
		}
		defer func() {
			_ = f.Close()
		}()

		// w is used to write the data, it will be wrapped with a tracker if needed
		var w io.Writer = f
		if tracker != nil {
			if resp.ContentLength != -1 {
				tracker.UpdateTotal(resp.ContentLength)
			}
			w = NewTrackerWriter(f, tracker)
		}

		_, err = io.Copy(w, resp.Body)
		return err
	}

	b := backoff.WithContext(newDownloadBackoff(), ctx)
	if err := backoff.Retry(operation, b); err != nil {
		return fmt.Errorf("failed to download object: %w", err)
	}
	return nil
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
