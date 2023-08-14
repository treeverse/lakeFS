package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/uri"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultDirectoryMask   = 0o755
	ClientMtimeMetadataKey = "x-client-mtime"
)

func getMtimeFromStats(stats api.ObjectStats) (int64, error) {
	if stats.Metadata == nil {
		return stats.Mtime, nil
	}
	clientMtime, hasClientMtime := stats.Metadata.Get(ClientMtimeMetadataKey)
	if hasClientMtime {
		// parse
		return strconv.ParseInt(clientMtime, 10, 64)
	}
	return stats.Mtime, nil
}

type Tasks struct {
	Downloaded uint64
	Uploaded   uint64
	Removed    uint64
}

type SyncManager struct {
	ctx            context.Context
	client         *api.ClientWithResponses
	httpClient     *http.Client
	progressBar    *ProgressPool
	maxParallelism int
	presign        bool
	tasks          Tasks
}

func NewSyncManager(ctx context.Context, client *api.ClientWithResponses, maxParallelism int, presign bool) *SyncManager {
	return &SyncManager{
		ctx:            ctx,
		client:         client,
		httpClient:     http.DefaultClient,
		progressBar:    NewProgressPool(),
		maxParallelism: maxParallelism,
		presign:        presign,
	}
}

// Sync - sync changes between remote and local directory given the Changes channel.
// For each change, will apply download, upload or delete according to the change type and change source
func (s *SyncManager) Sync(rootPath string, remote *uri.URI, changeSet <-chan *Change) error {
	s.progressBar.Start()
	defer s.progressBar.Stop()

	ch := make(chan bool, s.maxParallelism)
	for i := 0; i < s.maxParallelism; i++ {
		ch <- true
	}

	wg, ctx := errgroup.WithContext(s.ctx)
	done := false
	for change := range changeSet {
		<-ch // block until we have a slot
		c := change
		select {
		case <-ctx.Done():
			done = true
		default:
			wg.Go(func() error {
				return s.apply(ctx, rootPath, remote, c)
			})
		}
		ch <- true // release
		if done {
			break
		}
	}

	if err := wg.Wait(); err != nil {
		return err
	}
	_, err := fileutil.PruneEmptyDirectories(rootPath)
	return err
}

func (s *SyncManager) apply(ctx context.Context, rootPath string, remote *uri.URI, change *Change) (err error) {
	switch change.Type {
	case ChangeTypeAdded, ChangeTypeModified:
		switch change.Source {
		case ChangeSourceRemote:
			// remote changed something, download it!
			err = s.download(ctx, rootPath, remote, change)
			if err != nil {
				err = fmt.Errorf("download %s failed: %w", change.Path, err)
			}
			return err
		case ChangeSourceLocal:
			// we wrote something, upload it!
			err = s.upload(ctx, rootPath, remote, change)
			if err != nil {
				err = fmt.Errorf("upload %s failed: %w", change.Path, err)
			}
			return err
		default:
			panic("invalid change source")
		}
	case ChangeTypeRemoved:
		if change.Source == ChangeSourceRemote {
			// remote deleted something, delete it locally!
			return s.deleteLocal(rootPath, change)
		} else {
			// we deleted something, delete it on remote!
			return s.deleteRemote(ctx, remote, change)
		}
	case ChangeTypeConflict:
		return ErrConflict
	default:
		panic("invalid change type")
	}
}

func (s *SyncManager) download(ctx context.Context, rootPath string, remote *uri.URI, change *Change) error {
	if err := fileutil.VerifyRelPath(strings.TrimPrefix(change.Path, uri.PathSeparator), rootPath); err != nil {
		return err
	}
	destination := filepath.Join(rootPath, change.Path)
	destinationDirectory := filepath.Dir(destination)
	if err := os.MkdirAll(destinationDirectory, DefaultDirectoryMask); err != nil {
		return err
	}

	statResp, err := s.client.StatObjectWithResponse(ctx, remote.Repository, remote.Ref, &api.StatObjectParams{
		Path:         filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path)),
		Presign:      swag.Bool(s.presign),
		UserMetadata: swag.Bool(true),
	})
	if err != nil {
		return err
	}
	if statResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("%s (stat HTTP %d): %w", change.Path, statResp.StatusCode(), ErrDownloadingFile)
	}
	// get mtime
	mtimeSecs, err := getMtimeFromStats(*statResp.JSON200)
	if err != nil {
		return err
	}

	if strings.HasSuffix(change.Path, uri.PathSeparator) {
		// Directory marker - skip
		return nil
	}

	lastModified := time.Unix(mtimeSecs, 0)
	sizeBytes := swag.Int64Value(statResp.JSON200.SizeBytes)
	f, err := os.Create(destination)
	if err != nil {
		// sometimes we get a file that is actually a directory marker.
		// spark loves writing those. If we already have the directory we can skip it.
		if errors.Is(err, syscall.EISDIR) && sizeBytes == 0 {
			return nil // no further action required!
		}
		return fmt.Errorf("could not create file '%s': %w", destination, err)
	}
	defer func() {
		err = f.Close()
	}()

	if sizeBytes == 0 { // if size is empty just create file
		spinner := s.progressBar.AddSpinner(fmt.Sprintf("download %s", change.Path))
		defer spinner.Done()
	} else { // Download file
		// make request
		var body io.Reader
		if s.presign {
			resp, err := s.httpClient.Get(statResp.JSON200.PhysicalAddress)
			if err != nil {
				return err
			}
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("%s (pre-signed GET: HTTP %d): %w", change.Path, resp.StatusCode, ErrDownloadingFile)
			}
			body = resp.Body
		} else {
			resp, err := s.client.GetObject(ctx, remote.Repository, remote.Ref, &api.GetObjectParams{
				Path: filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path)),
			})
			if err != nil {
				return err
			}
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("%s (GetObject: HTTP %d): %w", change.Path, resp.StatusCode, ErrDownloadingFile)
			}
			body = resp.Body
		}

		b := s.progressBar.AddReader(fmt.Sprintf("download %s", change.Path), sizeBytes)
		barReader := b.Reader(body)
		defer func() {
			if err != nil {
				b.Error()
			} else {
				b.Done()
			}
		}()
		_, err = io.Copy(f, barReader)

		if err != nil {
			return fmt.Errorf("could not write file '%s': %w", destination, err)
		}
	}

	atomic.AddUint64(&s.tasks.Downloaded, 1)
	// set mtime to the server returned one
	err = os.Chtimes(destination, time.Now(), lastModified) // Explicit to catch in defer func
	return err
}

func (s *SyncManager) upload(ctx context.Context, rootPath string, remote *uri.URI, change *Change) error {
	source := filepath.Join(rootPath, change.Path)
	if err := fileutil.VerifySafeFilename(source); err != nil {
		return err
	}
	dest := filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path))

	f, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	fileStat, err := f.Stat()
	if err != nil {
		return err
	}

	b := s.progressBar.AddReader(fmt.Sprintf("upload %s", change.Path), fileStat.Size())
	defer func() {
		if err != nil {
			b.Error()
		} else {
			b.Done()
		}
	}()

	metadata := map[string]string{
		ClientMtimeMetadataKey: strconv.FormatInt(fileStat.ModTime().Unix(), 10),
	}

	reader := fileWrapper{
		file:   f,
		reader: b.Reader(f),
	}
	if s.presign {
		_, err = helpers.ClientUploadPreSign(
			ctx, s.client, remote.Repository, remote.Ref, dest, metadata, "", reader)
		return err
	}
	// not pre-signed
	_, err = helpers.ClientUploadDirect(
		ctx, s.client, remote.Repository, remote.Ref, dest, metadata, "", reader)
	return err
}

func (s *SyncManager) deleteLocal(rootPath string, change *Change) (err error) {
	b := s.progressBar.AddSpinner(fmt.Sprintf("delete local: %s", change.Path))
	defer func() {
		defer func() {
			if err != nil {
				b.Error()
			} else {
				b.Done()
			}
		}()
	}()
	source := filepath.Join(rootPath, change.Path)
	err = fileutil.RemoveFile(source)
	if err != nil {
		b.Error()
		return err
	}

	b.Done()
	atomic.AddUint64(&s.tasks.Removed, 1)
	return nil
}

func (s *SyncManager) deleteRemote(ctx context.Context, remote *uri.URI, change *Change) (err error) {
	b := s.progressBar.AddSpinner(fmt.Sprintf("delete remote path: %s", change.Path))
	defer func() {
		if err != nil {
			b.Error()
		} else {
			b.Done()
		}
	}()
	dest := filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path))
	resp, err := s.client.DeleteObjectWithResponse(ctx, remote.Repository, remote.Ref, &api.DeleteObjectParams{
		Path: dest,
	})
	if err != nil {
		return
	}
	if resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("could not delete object: HTTP %d: %w", resp.StatusCode(), helpers.ErrRequestFailed)
	}
	return
}

func (s *SyncManager) Summary() Tasks {
	return s.tasks
}
