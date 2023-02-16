package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSWalker struct {
	client *storage.Client
	mark   Mark
}

func NewGCSWalker(client *storage.Client) *GCSWalker {
	return &GCSWalker{client: client}
}

func (w *GCSWalker) Walk(ctx context.Context, storageURI *url.URL, op WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	prefix := strings.TrimLeft(storageURI.Path, "/")
	var basePath string
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		basePath = prefix[:idx+1]
	}
	iter := w.client.
		Bucket(storageURI.Host).
		Objects(ctx, &storage.Query{
			Prefix:      prefix,
			StartOffset: op.After,
		})

	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("error listing objects at storage uri %s: %w", storageURI, err)
		}

		// skipping first key (without forgetting the possible empty string key!)
		if op.After != "" && attrs.Name <= op.After {
			continue
		}

		w.mark = Mark{
			LastKey: attrs.Name,
			HasMore: true,
		}
		if err := walkFn(ObjectStoreEntry{
			FullKey:     attrs.Name,
			RelativeKey: strings.TrimPrefix(attrs.Name, basePath),
			Address:     fmt.Sprintf("gs://%s/%s", attrs.Bucket, attrs.Name),
			ETag:        hex.EncodeToString(attrs.MD5),
			Mtime:       attrs.Updated,
			Size:        attrs.Size,
		}); err != nil {
			return err
		}
	}
	w.mark = Mark{
		LastKey: "",
		HasMore: false,
	}

	return nil
}

func (w *GCSWalker) Marker() Mark {
	return w.mark
}
