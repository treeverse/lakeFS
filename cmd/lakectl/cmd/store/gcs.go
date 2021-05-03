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

func GetGCSClient(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx)
}

type GCSWalker struct {
	client *storage.Client
}

func (w *GCSWalker) Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error {
	prefix := strings.TrimLeft(storageURI.Path, "/")
	iter := w.client.
		Bucket(storageURI.Host).
		Objects(ctx, &storage.Query{Prefix: prefix})

	for {
		attrs, err := iter.Next()

		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("error listing objects at storage uri %s: %w", storageURI, err)
		}

		if err := walkFn(ObjectStoreEntry{
			FullKey:     attrs.Name,
			RelativeKey: strings.TrimPrefix(attrs.Name, prefix),
			Address:     fmt.Sprintf("gs://%s/%s", attrs.Bucket, attrs.Name),
			ETag:        hex.EncodeToString(attrs.MD5),
			Mtime:       attrs.Updated,
			Size:        attrs.Size,
		}); err != nil {
			return err
		}
	}

	return nil
}
