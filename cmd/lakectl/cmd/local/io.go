package local

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-openapi/swag"
	"gopkg.in/yaml.v3"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	DefaultFileMask      = 0o644
	DefaultDirectoryMask = 0o755
	IndexFileName        = ".index.yaml"
	EmptyFileHash        = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	PathSeparator        = "/"
)

var (
	ErrNotDirectory  = errors.New("path is not a directory")
	ErrNotFile       = errors.New("path is not a file")
	ErrNoCommitFound = errors.New("no commit found")
)

var ProgressBar pb.ProgressBarTemplate = `{{with string . "prefix"}}{{.}} {{end}}{{counters . }} {{bar . "[" "=" ">" "-" "]"}} {{percent . }}{{with string . "suffix"}} {{.}}{{end}}`

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

func RemoveDirectoryTree(p string) error {
	dirExists, err := DirectoryExists(p)
	if err != nil {
		return err
	}
	if dirExists {
		return nil // already exists!
	}
	return os.RemoveAll(p)
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

func GetFileSha1(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = f.Close()
	}()
	h := sha1.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func DownloadRemoteObject(object api.ObjectStats, prefix, localDirectory string, bar *pb.ProgressBar, indexer *ObjectTracker, entry *Object) (bool, error) {
	currentPath := strings.TrimPrefix(object.Path, prefix)
	fetchURL := object.PhysicalAddress

	if _, base := path.Split(currentPath); base == "" {
		return false, nil // files with empty name are usually directories - a normal filesystem cannot support this!
	}

	// create required directory structure if missing
	localObjectPath := path.Join(localDirectory, currentPath)
	dir := path.Dir(localObjectPath)
	err := CreateDirectoryTree(dir)
	if err != nil {
		return false, err
	}

	info, err := os.Stat(localObjectPath)
	shouldDownload := false
	if os.IsNotExist(err) {
		// no such file
		shouldDownload = true
	} else if err != nil {
		return false, fmt.Errorf("%w: could not stat local file: %s", err, localObjectPath)
	} else if api.Int64Value(object.SizeBytes) == 0 && info.Size() != 0 {
		// remote object is empty but ours isn't!
		shouldDownload = true
	} else if api.Int64Value(object.SizeBytes) == 0 {
		// both empty
		shouldDownload = false
	} else {
		// size is not zero, object exists! let's compare
		if info.Size() != swag.Int64Value(object.SizeBytes) {
			// if the object has a different size, it's definitely different
			shouldDownload = true
		} else if math.Abs(time.Unix(object.Mtime, 0).Sub(info.ModTime()).Seconds()) > 0 {
			// same size but different modification date
			hashCode, err := GetFileSha1(localObjectPath)
			if err != nil {
				return false, err
			}
			if entry == nil || entry.Sha1 != hashCode {
				shouldDownload = true // we don't have a hash to compare with, let's re-download
			}
		} else {
			// same size, same modification date - we won't download again.
			// we also don't calculate the checksum. Heuristically, this is the same file.
			shouldDownload = false
		}
	}

	if !shouldDownload {
		bar.SetTotal(100)
		bar.SetCurrent(100)
		bar.Set("prefix", currentPath)
		var hashCode string
		if entry != nil {
			hashCode = entry.Sha1
		} else if api.Int64Value(object.SizeBytes) == 0 {
			hashCode = EmptyFileHash
		} else {
			sha, err := GetFileSha1(localObjectPath)
			if err != nil {
				return false, err
			}
			hashCode = sha
		}
		indexer.AddExisting(currentPath, hashCode, api.Int64Value(object.SizeBytes), object.Mtime)
		return true, nil // done!
	}

	// TODO(ozk): retry this with exponential backoff?
	resp, err := http.Get(fetchURL)
	if err != nil {
		return false, fmt.Errorf("%w: could not download file '%s'", err, localObjectPath)
	}
	bar.SetTotal(resp.ContentLength)
	bar.SetCurrent(0)
	bar.Set("prefix", currentPath)
	barReader := bar.NewProxyReader(resp.Body)

	f, err := os.Create(localObjectPath)
	if err != nil {
		return false, fmt.Errorf("%w: could not create file '%s'", err, localObjectPath)
	}
	err = indexer.Add(currentPath, object.Mtime, api.Int64Value(object.SizeBytes), barReader, f)
	if err != nil {
		_ = f.Close()
		return false, fmt.Errorf("%w: could not write downloaded file '%s'", err, localObjectPath)
	}
	err = f.Close()
	if err != nil {
		return false, err
	}
	return false, os.Chtimes(localObjectPath, time.Now(), time.Unix(object.Mtime, 0))
}

func HasParentDir(p, d string) bool {
	dir := path.Dir(p)
	for dir != "" && dir != "." {
		if d == dir {
			return true
		}
		dir = path.Dir(dir)
	}
	return false
}

type Diff struct {
	Added    []string
	Removed  []string
	Modified []string
}

func NewLocalDiff() *Diff {
	return &Diff{
		Added:    make([]string, 0),
		Removed:  make([]string, 0),
		Modified: make([]string, 0),
	}
}

func (d *Diff) IsClean() bool {
	return len(d.Added) == 0 && len(d.Modified) == 0 && len(d.Removed) == 0
}

func DoDiff(localDirectory string) (*Diff, error) {
	list := NewLocalDiff()
	index, err := ReadIndex(localDirectory)
	if err != nil {
		return nil, err
	}
	if index == nil {
		return nil, fmt.Errorf("no index file found for directory '%s'", localDirectory)
	}
	visited := make(map[string]bool)
	fileSystem := os.DirFS(localDirectory)
	err = fs.WalkDir(fileSystem, ".", func(p string, d fs.DirEntry, err error) error {
		if p == "." || p == IndexFileName {
			return nil // ignore index and root dir
		}
		fullPath := path.Join(localDirectory, p)
		visited[p] = true
		if d.IsDir() {
			return nil // TODO (ozk): also sync empty directories
		}
		entry := index.Find(p)
		if entry == nil {
			// this file is new!
			list.Added = append(list.Added, p)
			return nil
		}
		// we have an entry, this is either the same or modified ;)
		info, err := os.Stat(fullPath)
		if err != nil {
			return fmt.Errorf("could not stat file %s: %w", fullPath, err)
		}
		if entry.SizeBytes != info.Size() {
			list.Modified = append(list.Modified, p)
			return nil
		}
		if math.Abs(time.Unix(entry.Mtime, 0).Sub(info.ModTime()).Seconds()) > 0 {
			hashCode, err := GetFileSha1(fullPath)
			if err != nil {
				return err
			}
			if entry.Sha1 != hashCode {
				list.Modified = append(list.Modified, p)
			}
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// now let's go over anything that might have been deleted
	for _, obj := range index.Objects {
		if _, exists := visited[obj.Path]; !exists {
			list.Removed = append(list.Removed, obj.Path)
		}
	}

	return list, nil
}

func uploadDirectoryChangesConcurrent(ctx context.Context, client api.ClientWithResponsesInterface, dest *uri.URI, localDirectory string, maxParallelism int) error {
	// find changes
	diff, err := DoDiff(localDirectory)
	if err != nil {
		return err
	}
	if diff.IsClean() {
		return nil
	}

	totalActions := len(diff.Modified) + len(diff.Removed) + len(diff.Added)
	parallelDownloads := maxParallelism
	if totalActions < parallelDownloads {
		parallelDownloads = totalActions
	}

	bars := make([]*pb.ProgressBar, parallelDownloads)
	for i := range bars {
		bars[i] = ProgressBar.New(0)
	}
	pool := pb.NewPool()
	mainBar := ProgressBar.New(totalActions)
	mainBar.Set("prefix", "applying diff")
	pool.Add(mainBar)
	pool.Add(bars...)
	if err := pool.Start(); err != nil {
		return err
	}

	ch := make(chan *pb.ProgressBar, parallelDownloads)
	for _, bar := range bars {
		ch <- bar
	}
	errCh := make(chan error, 0)
	doneCh := make(chan bool, 0)
	var wg sync.WaitGroup
	wg.Add(totalActions)

	// upload them
	for _, d := range diff.Added {
		go func(d string) {
			bar := <-ch
			filePath := path.Join(localDirectory, d)
			bar.Set("prefix", fmt.Sprintf("Adding %s", d))
			err = WriteLakeFSFileWithProgress(ctx, client, filePath, &uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			}, bar)
			if err != nil {
				errCh <- err
			} else {
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}
	for _, d := range diff.Modified {
		go func(d string) {
			bar := <-ch
			filePath := path.Join(localDirectory, d)
			bar.Set("prefix", fmt.Sprintf("Replacing %s", d))
			err = WriteLakeFSFileWithProgress(ctx, client, filePath, &uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			}, bar)
			if err != nil {
				errCh <- err
			} else {
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}
	for _, d := range diff.Removed {
		go func(d string) {
			bar := <-ch
			bar.Set("prefix", fmt.Sprintf("Deleting %s", path.Join(dest.GetPath(), d)))
			bar.SetTotal(100)
			bar.SetCurrent(0)
			err = DeleteLakeFSFile(ctx, client, &uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			})
			if err != nil {
				errCh <- err
			} else {
				bar.SetCurrent(100)
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}

	go func() {
		wg.Wait() // wait until all downloads are done
		doneCh <- true
	}()

	select {
	case <-doneCh:
		break
	case err := <-errCh:
		return err
	}

	return pool.Stop()
}

func UploadDirectoryChanges(ctx context.Context, client api.ClientWithResponsesInterface, dest *uri.URI, localDirectory string, maxParallelism int) error {
	if maxParallelism > 1 {
		return uploadDirectoryChangesConcurrent(ctx, client, dest, localDirectory, maxParallelism)
	}
	// find changes
	diff, err := DoDiff(localDirectory)
	if err != nil {
		return err
	}
	if diff.IsClean() {
		return nil
	}
	// upload them
	for _, d := range diff.Added {
		filePath := path.Join(localDirectory, d)
		err = WriteLakeFSFile(ctx, client, filePath, &uri.URI{
			Repository: dest.Repository,
			Ref:        dest.Ref,
			Path:       swag.String(path.Join(dest.GetPath(), d)),
		})
		if err != nil {
			return err
		}
	}
	for _, d := range diff.Modified {
		filePath := path.Join(localDirectory, d)
		err = WriteLakeFSFile(ctx, client, filePath, &uri.URI{
			Repository: dest.Repository,
			Ref:        dest.Ref,
			Path:       swag.String(path.Join(dest.GetPath(), d)),
		})
		if err != nil {
			return err
		}
	}
	for _, d := range diff.Removed {
		err = DeleteLakeFSFile(ctx, client, &uri.URI{
			Repository: dest.Repository,
			Ref:        dest.Ref,
			Path:       swag.String(path.Join(dest.GetPath(), d)),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteIndex(directory string, index *Index) error {
	data, err := yaml.Marshal(index)
	if err != nil {
		return err
	}
	return os.WriteFile(path.Join(directory, IndexFileName), data, DefaultFileMask)
}

func ReadIndex(directory string) (*Index, error) {
	indexLoc := path.Join(directory, IndexFileName)
	fileExists, err := FileExists(indexLoc)
	if err != nil {
		return nil, err
	}
	if !fileExists {
		return nil, nil
	}
	data, err := os.ReadFile(indexLoc)
	if err != nil {
		return nil, err
	}
	index := &Index{}
	err = yaml.Unmarshal(data, index)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func InitEmptyIndex(source *uri.URI, localDirectory string) error {
	return WriteIndex(
		localDirectory,
		&Index{
			Ref:     source.String(),
			Objects: (&ObjectTracker{}).GetObjects(),
		},
	)
}

func SyncDirectory(ctx context.Context, client api.ClientWithResponsesInterface, source *uri.URI, localDirectory string, maxParallelism int) error {
	indexer := &ObjectTracker{}

	fileSet := make(map[string]bool)

	hasMore := true
	after := ""
	prefix := source.GetPath()
	// we're syncing a directory, let's make sure we normalize it by adding a '/' at the end:
	if !strings.HasSuffix(prefix, PathSeparator) {
		prefix = prefix + PathSeparator
	}

	// list the things
	filesWeNeed := make([]api.ObjectStats, 0)
	for hasMore {
		listing, err := client.ListObjectsWithResponse(ctx, source.Repository, source.Ref, &api.ListObjectsParams{
			Presign: swag.Bool(true), // TODO(ozk): can we still support Azure? local?
			After:   (*api.PaginationAfter)(swag.String(after)),
			Prefix:  (*api.PaginationPrefix)(swag.String(prefix)),
		})
		if err != nil {
			return err
		}
		if listing.StatusCode() != http.StatusOK {
			return fmt.Errorf("could not list remote ref '%s': HTTP %d", source.Ref, listing.StatusCode())
		}

		for _, object := range listing.JSON200.Results {
			filesWeNeed = append(filesWeNeed, object)
		}

		hasMore = listing.JSON200.Pagination.HasMore
		after = listing.JSON200.Pagination.NextOffset
	}

	parallelDownloads := maxParallelism
	if len(filesWeNeed) < parallelDownloads {
		parallelDownloads = len(filesWeNeed)
	}

	bars := make([]*pb.ProgressBar, parallelDownloads)
	for i := range bars {
		bars[i] = ProgressBar.New(0)
	}
	pool := pb.NewPool()
	mainBar := ProgressBar.New(len(filesWeNeed))
	mainBar.Set("prefix", fmt.Sprintf("objects in '%s'", source.String()))
	pool.Add(mainBar)
	pool.Add(bars...)
	if err := pool.Start(); err != nil {
		return err
	}

	ch := make(chan *pb.ProgressBar, parallelDownloads)
	for _, bar := range bars {
		ch <- bar
	}
	errCh := make(chan error, 0)
	doneCh := make(chan bool, 0)
	var wg sync.WaitGroup
	wg.Add(len(filesWeNeed))

	fileSetLock := sync.RWMutex{}
	skippedObjects := 0

	// ensure data directory exists
	err := CreateDirectoryTree(localDirectory)
	if err != nil {
		return err
	}
	fileSystem := os.DirFS(localDirectory)
	// current index:
	index, err := ReadIndex(localDirectory)
	if err != nil {
		return err
	}

	// download the things
	for _, obj := range filesWeNeed {
		bar := <-ch // block until we have a slot
		go func(obj api.ObjectStats) {
			currentPath := strings.TrimPrefix(obj.Path, prefix)
			var entry *Object
			if index != nil {
				// read this path from index
				entry = index.Find(currentPath)
			}
			skipped, err := DownloadRemoteObject(obj, prefix, localDirectory, bar, indexer, entry)
			if err != nil {
				errCh <- err
				return
			}
			// update fileset
			fileSetLock.Lock()
			if skipped {
				skippedObjects += 1
			}
			fileSet[currentPath] = true // add to download set
			// recursively also add all parents
			d := path.Dir(currentPath)
			for d != "" && d != "." && d != "/" {
				fileSet[d] = true
				d = path.Dir(d)
			}
			fileSetLock.Unlock()
			mainBar.Increment()
			wg.Done()
			ch <- bar // release
		}(obj)
	}
	go func() {
		wg.Wait() // wait until all downloads are done
		doneCh <- true
	}()

	select {
	case <-doneCh:
		break
	case err := <-errCh:
		return err
	}

	if err := pool.Stop(); err != nil {
		return err
	}

	fmt.Printf("Finishing syncing directory '%s' (%d/%d skipped)\n\n", localDirectory, skippedObjects, len(filesWeNeed))

	// iterate over local files now
	skipPrefix := ""
	err = fs.WalkDir(fileSystem, ".", func(p string, d fs.DirEntry, err error) error {
		if p == "." || p == IndexFileName {
			return nil
		}
		if skipPrefix != "" && HasParentDir(p, skipPrefix) {
			// we can skip as we removed a parent folder
			return nil
		}
		fullPath := path.Join(localDirectory, p)
		_, written := fileSet[p]
		if !written && d.IsDir() {
			err = RemoveDirectoryTree(fullPath)
			if err != nil {
				return err
			}
		} else if !written {
			err = RemoveFile(fullPath)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// write the index
	return WriteIndex(
		localDirectory,
		&Index{
			Ref:     source.String(),
			Objects: indexer.GetObjects(),
		},
	)
}
