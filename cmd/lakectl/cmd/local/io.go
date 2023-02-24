package local

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/api"
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
	attempt := 0
	var loopErr error
	for attempt <= 3 {
		attempt++
		resp, err := http.Get(fetchURL)
		if err != nil {
			loopErr = fmt.Errorf("%w: could not download file '%s'", err, localObjectPath)
			continue // try again?
		}
		bar.SetTotal(resp.ContentLength)
		bar.SetCurrent(0)
		bar.Set("prefix", fmt.Sprintf("%s [#%d]", currentPath, attempt))
		barReader := bar.NewProxyReader(resp.Body)
		f, err := os.Create(localObjectPath)
		if err != nil {
			loopErr = fmt.Errorf("%w: could not create file '%s'", err, localObjectPath)
			continue
		}
		err = indexer.Add(currentPath, object.Mtime, api.Int64Value(object.SizeBytes), barReader, f)
		if err != nil {
			_ = f.Close()
			loopErr = fmt.Errorf("%w: could not write downloaded file '%s'", err, localObjectPath)
			continue
		}
		_ = f.Close()
		err = os.Chtimes(localObjectPath, time.Now(), time.Unix(object.Mtime, 0))
		if err != nil {
			loopErr = err
			continue
		}
		break
	}
	return false, loopErr
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
