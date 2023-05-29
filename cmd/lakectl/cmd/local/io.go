package local

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/helpers"
	"github.com/treeverse/lakefs/pkg/uri"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DefaultDirectoryMask   = 0o755
	ClientMtimeMetadataKey = "x-client-mtime"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrNotDirectory    = errors.New("path is not a directory")
	ErrNotFile         = errors.New("path is not a file")
	ErrDownloadingFile = errors.New("error downloading file")
)

func FindInParents(path, filename string) (string, error) {
	var lookup string
	fullPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for fullPath != string(filepath.Separator) && fullPath != filepath.VolumeName(fullPath) {
		info, err := os.Stat(fullPath)
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%s: %w", fullPath, ErrNotFound)
		} else if err != nil {
			return "", err
		}
		if !info.IsDir() {
			// find filename here
			lookup = filepath.Join(filepath.Dir(fullPath), filename)
		} else {
			lookup = filepath.Join(fullPath, filename)
		}
		_, err = os.Stat(lookup)
		if os.IsNotExist(err) {
			fullPath = filepath.Dir(fullPath)
			continue
		} else if err != nil {
			return "", err
		}
		return lookup, nil
	}
	return "", nil
}

func FileExists(p string) (bool, error) {
	info, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return true, nil
	}
	return false, fmt.Errorf("%s: %w", p, ErrNotFile)
}

func DirectoryExists(p string) (bool, error) {
	info, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if info.IsDir() {
		return true, nil
	}
	return false, fmt.Errorf("%s: %w", p, ErrNotDirectory)
}

func CreateDirectoryTree(p string) error {
	dirExists, err := DirectoryExists(p)
	if err != nil {
		return err
	}
	if dirExists {
		return nil // already exists!
	}
	return os.MkdirAll(p, DefaultDirectoryMask)
}

func RemoveFile(p string) error {
	fileExists, err := FileExists(p)
	if err != nil {
		return err
	}
	if !fileExists {
		return nil // does not exist
	}
	return os.Remove(p)
}

type f struct {
	path string
	d    fs.DirEntry
}

func isParentDir(parent, sub string) (bool, error) {
	up := ".." + string(os.PathSeparator)

	// path-comparisons using filepath.Abs don't work reliably according to docs (no unique representation).
	rel, err := filepath.Rel(parent, sub)
	if err != nil {
		return false, err
	}
	if !strings.HasPrefix(rel, up) && rel != ".." {
		return true, nil
	}
	return false, nil
}

// PruneEmptyDirs iterates through the directory tree, removing empty directories, and directories that only
// contain empty directories. This implementation is not efficient.
func PruneEmptyDirs(p string) ([]string, error) {
	dirs := make(map[string]bool)
	haveFiles := make(map[string]bool)
	// when can we delete:
	err := filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
		if path == p {
			return nil // skip parent
		}
		if d.IsDir() {
			// dir
			dirs[path] = true
		} else {
			// file
			haveFiles[filepath.Dir(path)] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// so now we have a list of dirs with files, we want to keep those
	for k, _ := range haveFiles {
		currDir := k
		for len(currDir) > len(p) {
			delete(dirs, currDir)
			currDir = filepath.Dir(currDir)
		}
	}
	// we're left with those that don't have files, let's remove them.
	toRemove := make([]string, len(dirs))
	i := 0
	for k, _ := range dirs {
		toRemove[i] = k
		i++
	}
	sort.Strings(toRemove)
	// remove directories whose parents are removed anyway
	toRemoveFiltered := make([]string, 0)
	for i := 0; i < len(toRemove); i++ {
		// is there a previous dir?
		if i == 0 {
			// no parent dir
			toRemoveFiltered = append(toRemoveFiltered, toRemove[i])
			continue
		}
		// look back
		if len(toRemoveFiltered) > 0 {
			prevRemoved := toRemoveFiltered[len(toRemoveFiltered)-1]
			isParent, err := isParentDir(prevRemoved, toRemove[i])
			if err != nil {
				return nil, err
			}
			if !isParent {
				toRemoveFiltered = append(toRemoveFiltered, toRemove[i])
			}
		}
	}
	// actually prune
	for _, target := range toRemoveFiltered {
		err = os.RemoveAll(target)
		if err != nil {
			return toRemoveFiltered, err
		}
	}
	return toRemoveFiltered, nil
}

func NewSyncManager(ctx context.Context, client *api.ClientWithResponses, maxParallelism int, presign bool) *SyncManager {
	return &SyncManager{
		ctx:            ctx,
		client:         client,
		httpClient:     http.DefaultClient,
		maxParallelism: maxParallelism,
		presign:        presign,
	}
}

type SyncManager struct {
	ctx            context.Context
	client         *api.ClientWithResponses
	httpClient     *http.Client
	maxParallelism int
	presign        bool
}

func (s *SyncManager) download(rootPath string, remote *uri.URI, change *Change, bar *ProgressPool) error {
	destination := filepath.Join(rootPath, change.Path)
	destinationDirectory := filepath.Dir(destination)
	if err := CreateDirectoryTree(destinationDirectory); err != nil {
		return err
	}

	var lastModified time.Time
	var sizeBytes int64
	statResp, err := s.client.StatObjectWithResponse(s.ctx, remote.Repository, remote.Ref, &api.StatObjectParams{
		Path:         filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path)),
		Presign:      swag.Bool(s.presign),
		UserMetadata: swag.Bool(true),
	})
	if err != nil {
		return err
	}
	if statResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("%w: %s (stat HTTP %d)", ErrDownloadingFile, change.Path, statResp.StatusCode())
	}
	// get mtime
	mtimeSecs, err := getMtimeFromStats(*statResp.JSON200)
	if err != nil {
		return err
	}
	lastModified = time.Unix(mtimeSecs, 0)

	// get size
	sizeBytes = swag.Int64Value(statResp.JSON200.SizeBytes)

	// if size is empty, or directory marker, do nothing!
	if sizeBytes == 0 {
		spinner := bar.AddSpinner(fmt.Sprintf("downloading %s", change.Path))
		defer spinner.Done()
		if strings.HasSuffix(change.Path, PathSeparator) {
			return nil
		}
		// not a directory marker, just an empty file
		f, err := os.Create(destination)
		if err != nil {
			return err
		}
		err = f.Close()
		if err != nil {
			return err
		}
		// set mtime to the server returned one
		return os.Chtimes(destination, time.Now(), lastModified)
	}

	// make request
	var body io.Reader
	if s.presign {
		resp, err := s.httpClient.Get(statResp.JSON200.PhysicalAddress)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%w: %s (pre-signed GET: HTTP %d)", ErrDownloadingFile, change.Path, resp.StatusCode)
		}
		body = resp.Body
	} else {
		resp, err := s.client.GetObject(s.ctx, remote.Repository, remote.Ref, &api.GetObjectParams{
			Path: filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path)),
		})
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%w: %s (GetObject: HTTP %d)", ErrDownloadingFile, change.Path, resp.StatusCode)
		}
		body = resp.Body
	}

	f, err := os.Create(destination)
	if err != nil {
		// sometimes we get a file that is actually a directory marker.
		// spark loves writing those. If we already have the directory we can skip it.
		if errors.Is(err, syscall.EISDIR) && sizeBytes == 0 {
			return nil // no further action required!
		}
		return fmt.Errorf("%w: could not create file '%s'", err, destination)
	}
	b := bar.AddReader(fmt.Sprintf("downloading %s", change.Path), sizeBytes)
	barReader := b.Reader(body)
	defer b.Done()

	_, err = io.Copy(f, barReader)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: could not write file '%s'", err, destination)
	}

	err = f.Close()
	if err != nil {
		return err
	}

	// set mtime to the server returned one
	return os.Chtimes(destination, time.Now(), lastModified)
}

func (s *SyncManager) upload(rootPath string, remote *uri.URI, change *Change, bar *ProgressPool) error {
	source := filepath.Join(rootPath, change.Path)
	dest := filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path))

	f, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	n, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.New("could not get file size")
	}
	b := bar.AddReader(fmt.Sprintf("uploading %s", change.Path), n)
	defer b.Done()
	reader := fileWrapper{
		file:   f,
		reader: b.Reader(f),
	}

	fileStat, err := f.Stat()
	if err != nil {
		return err
	}
	metadata := map[string]string{
		ClientMtimeMetadataKey: strconv.FormatInt(fileStat.ModTime().Unix(), 10),
	}
	if s.presign {
		_, err = helpers.ClientUploadPreSign(
			s.ctx, s.client, remote.Repository, remote.Ref, dest, metadata, "", reader)
		return err
	}
	// not pre-signed
	_, err = helpers.ClientUploadDirect(
		s.ctx, s.client, remote.Repository, remote.Ref, dest, metadata, "", reader)
	return err
}

func (s *SyncManager) deleteLocal(rootPath string, remote *uri.URI, change *Change, bar *ProgressPool) error {
	b := bar.AddSpinner(fmt.Sprintf("delete local path: %s", change.Path))
	source := filepath.Join(rootPath, change.Path)
	err := RemoveFile(source)
	if err != nil {
		return err
	}
	b.Done()
	return nil
}

func (s *SyncManager) deleteRemote(rootPath string, remote *uri.URI, change *Change, bar *ProgressPool) error {
	b := bar.AddSpinner(fmt.Sprintf("delete remote path: %s", change.Path))
	dest := filepath.ToSlash(filepath.Join(remote.GetPath(), change.Path))
	resp, err := s.client.DeleteObjectWithResponse(s.ctx, remote.Repository, remote.Ref, &api.DeleteObjectParams{
		Path: dest,
	})
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("could not delete object: HTTP %d", resp.StatusCode())
	}
	b.Done()
	return nil
}

func (s *SyncManager) apply(rootPath string, remote *uri.URI, change *Change, bar *ProgressPool) error {
	switch change.Type {
	case "added", "changed":
		if change.Source == "remote" {
			// remote changed something, download it!
			return s.download(rootPath, remote, change, bar)
		} else {
			// we wrote something, upload it!
			return s.upload(rootPath, remote, change, bar)
		}
	case "removed":
		if change.Source == "remote" {
			// remote deleted something, delete it locally!
			return s.deleteLocal(rootPath, remote, change, bar)
		} else {
			// we deleted something, delete it on remote!
			return s.deleteRemote(rootPath, remote, change, bar)
		}
	case "conflict":
		return ErrConflict
	}
	return ErrChangeType
}

func (s *SyncManager) Sync(rootPath string, remote *uri.URI, changeSet Changes) error {
	if len(changeSet) == 0 {
		return nil
	}
	parallelOps := s.maxParallelism
	if len(changeSet) < parallelOps {
		parallelOps = len(changeSet)
	}

	pool := NewProgressPool(len(changeSet))
	pool.Start()

	ch := make(chan bool, parallelOps)
	for i := 0; i < parallelOps; i++ {
		ch <- true
	}
	errCh := make(chan error, 0)
	doneCh := make(chan bool, 0)
	var wg sync.WaitGroup
	wg.Add(len(changeSet))

	// download the things
	for _, op := range changeSet {
		<-ch // block until we have a slot
		go func(op *Change) {
			err := s.apply(rootPath, remote, op, pool)
			if err != nil {
				errCh <- err
				return
			}
			wg.Done()
			ch <- true // release
		}(op)
	}
	go func() {
		wg.Wait() // wait until all downloads are done
		doneCh <- true
	}()

	select {
	case <-doneCh:
		pool.Stop()
		_, err := PruneEmptyDirs(rootPath)
		return err
	case err := <-errCh:
		pool.Stop()
		return err
	}
	return nil
}
