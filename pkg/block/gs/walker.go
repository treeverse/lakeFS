package gs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/treeverse/lakefs/pkg/block"
	"google.golang.org/api/iterator"
)

type GCSWalker struct {
	client *storage.Client
	mark   block.Mark
}

func NewGCSWalker(client *storage.Client) *GCSWalker {
	return &GCSWalker{client: client}
}

func (w *GCSWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
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

		w.mark = block.Mark{
			LastKey: attrs.Name,
			HasMore: true,
		}
		if err := walkFn(block.ObjectStoreEntry{
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
	w.mark = block.Mark{
		LastKey: "",
		HasMore: false,
	}

	return nil
}

func (w *GCSWalker) Marker() block.Mark {
	return w.mark
}
