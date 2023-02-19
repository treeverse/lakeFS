package store

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/exp/slices"
)

type LocalWalker struct {
	mark            Mark
	allowedPrefixes []string
}

func NewLocalWalker(allowedPrefixes []string) *LocalWalker {
	return &LocalWalker{
		allowedPrefixes: allowedPrefixes,
		mark:            Mark{HasMore: true},
	}
}

func (l *LocalWalker) Walk(_ context.Context, storageURI *url.URL, options WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	if storageURI.Scheme != "local" {
		return path.ErrBadPattern
	}
	root := path.Join(storageURI.Host, storageURI.Path)
	if err := l.verifyAbsPath(root); err != nil {
		return err
	}
	var entries []*ObjectStoreEntry
	err := filepath.Walk(root, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		key := filepath.ToSlash(p)
		if key <= options.After {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		hash := md5.New() //nolint:gosec
		_, err = io.Copy(hash, f)
		if err != nil {
			return err
		}

		addr := "local://" + key
		relativePath, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		etag := hex.EncodeToString(hash.Sum(nil))
		ent := &ObjectStoreEntry{
			FullKey:     key,
			RelativeKey: filepath.ToSlash(relativePath),
			Address:     addr,
			ETag:        etag,
			Mtime:       info.ModTime(),
			Size:        info.Size(),
		}
		entries = append(entries, ent)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].FullKey < entries[j].FullKey
	})
	for _, ent := range entries {
		l.mark.LastKey = ent.FullKey
		if err := walkFn(*ent); err != nil {
			return err
		}
	}
	l.mark = Mark{}
	return nil
}

func (l *LocalWalker) verifyAbsPath(root string) error {
	if !filepath.IsAbs(root) {
		return ErrBadPath
	}
	if !slices.ContainsFunc(l.allowedPrefixes, func(prefix string) bool {
		return strings.HasPrefix(root, prefix)
	}) {
		return ErrForbidden
	}
	return nil
}

func (l *LocalWalker) Marker() Mark {
	return l.mark
}
