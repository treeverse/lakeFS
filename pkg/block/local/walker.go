package local

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
)

const cacheDirName = "_lakefs_cache"

type Walker struct {
	mark            block.Mark
	importHidden    bool
	allowedPrefixes []string
	cacheLocation   string
	path            string
}

func NewLocalWalker(params params.Local) *Walker {
	// without Path, we do not keep cache - will make walker very slow
	var cacheLocation string
	if params.Path != "" {
		cacheLocation = filepath.Join(params.Path, cacheDirName)
	}
	return &Walker{
		mark:            block.Mark{HasMore: true},
		importHidden:    params.ImportHidden,
		allowedPrefixes: params.AllowedExternalPrefixes,
		cacheLocation:   cacheLocation,
		path:            params.Path,
	}
}

func (l *Walker) Walk(_ context.Context, storageURI *url.URL, options block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	if storageURI.Scheme != "local" {
		return path.ErrBadPattern
	}
	root := path.Join(storageURI.Host, storageURI.Path)
	if err := VerifyAbsPath(root, l.path, l.allowedPrefixes); err != nil {
		return err
	}

	var entries []*block.ObjectStoreEntry
	// verify and use cache - location is stored in continuation token
	if options.ContinuationToken != "" && strings.HasPrefix(options.ContinuationToken, l.cacheLocation) {
		cacheData, err := os.ReadFile(options.ContinuationToken)
		if err == nil {
			err = json.Unmarshal(cacheData, &entries)
			if err != nil {
				entries = nil
			} else {
				l.mark.ContinuationToken = options.ContinuationToken
			}
		}
	}

	// if needed scan all entries to import and calc etag
	if entries == nil {
		var err error
		entries, err = l.scanEntries(root, options)
		if err != nil {
			return err
		}

		// store entries to cache file
		if l.cacheLocation != "" {
			jsonData, err := json.Marshal(entries)
			if err != nil {
				return err
			}
			const dirPerm = 0o755
			_ = os.MkdirAll(l.cacheLocation, dirPerm)
			cacheName := filepath.Join(l.cacheLocation, gonanoid.Must()+"-import.json")
			const cachePerm = 0o644
			if err := os.WriteFile(cacheName, jsonData, cachePerm); err != nil {
				return err
			}
			l.mark.ContinuationToken = cacheName
		}
	}

	// search start position base on Last key
	startIndex := sort.Search(len(entries), func(i int) bool {
		return entries[i].FullKey > options.After
	})
	for i := startIndex; i < len(entries); i++ {
		ent := *entries[i]
		etag, err := calcFileETag(ent)
		if err != nil {
			return err
		}

		ent.ETag = etag
		l.mark.LastKey = ent.FullKey
		if err := walkFn(ent); err != nil {
			return err
		}
	}
	// delete cache in case we completed the iteration
	if l.mark.ContinuationToken != "" {
		if err := os.Remove(l.mark.ContinuationToken); err != nil {
			return err
		}
	}
	l.mark = block.Mark{}
	return nil
}

func (l *Walker) scanEntries(root string, options block.WalkOptions) ([]*block.ObjectStoreEntry, error) {
	var entries []*block.ObjectStoreEntry
	if err := filepath.Walk(root, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// skip hidden files and directories
		if !l.importHidden && strings.HasPrefix(info.Name(), ".") {
			if info.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		key := filepath.ToSlash(p)
		if key < options.After {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		addr := "local://" + key
		relativePath, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		// etag is calculated during iteration
		ent := &block.ObjectStoreEntry{
			FullKey:     key,
			RelativeKey: filepath.ToSlash(relativePath),
			Address:     addr,
			Mtime:       info.ModTime(),
			Size:        info.Size(),
		}
		entries = append(entries, ent)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].FullKey < entries[j].FullKey
	})
	return entries, nil
}

func calcFileETag(ent block.ObjectStoreEntry) (string, error) {
	f, err := os.Open(ent.FullKey)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	hash := md5.New() //nolint:gosec
	_, err = io.Copy(hash, f)
	if err != nil {
		return "", err
	}
	etag := hex.EncodeToString(hash.Sum(nil))
	return etag, nil
}

func (l *Walker) Marker() block.Mark {
	return l.mark
}
