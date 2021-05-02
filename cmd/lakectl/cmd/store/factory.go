package store

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

type ObjectStoreEntry struct {
	FullKey     string
	RelativeKey string
	Address     string
	ETag        string
	Mtime       time.Time
	Size        int64
}

type Walker interface {
	Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error
}

func (e ObjectStoreEntry) String() string {
	return fmt.Sprintf("%s\t%s\t%s\t%d\t%s\n", e.Address, e.RelativeKey, e.ETag, e.Size, e.Mtime)
}

func Walk(ctx context.Context, storageURI string, walkFn func(e ObjectStoreEntry) error) error {
	var walker Walker
	uri, err := url.Parse(storageURI)
	if err != nil {
		return fmt.Errorf("could not parse storage URI %s: %v", uri, err)
	}
	switch uri.Scheme {
	case "s3":
		svc, err := GetS3Client()
		if err != nil {
			return err
		}
		walker = &S3Walker{s3: svc}
	case "gs":
		svc, err := GetGCSClient(ctx)
		if err != nil {
			return err
		}
		walker = &GCSWalker{client: svc}
	case "http", "https":
		svc, err := GetAzureClient()
		if err != nil {
			return err
		}
		walker = &AzureBlobWalker{client: svc}
	default:
		return fmt.Errorf("no matching object store adapter for scheme: %s", uri.Scheme)
	}
	return walker.Walk(ctx, uri, walkFn)
}
