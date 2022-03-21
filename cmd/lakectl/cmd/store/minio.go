package store

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/url"
	"os"
	"strings"
)

func GetMinioClient() (*minio.Client, error) {
	accountName, accessKey, secretKey := os.Getenv("MINIO_STORAGE_ACCOUNT"), os.Getenv("MINIO_STORAGE_ACCESS_KEY"), os.Getenv("MINIO_STORAGE_SECRET_ACCESS_KEY")
	if len(accountName) == 0 || len(accessKey) == 0 || len(secretKey) == 0 {
		return nil, fmt.Errorf("either the MINIO_STORAGE_ACCOUNT or MINIO_STORAGE_ACCESS_KEY environment variable is not set")
	}

	minioClient, err := minio.New(accountName, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("error instantiating client")
	}

	return minioClient, nil
}

type MinioWalker struct {
	client *minio.Client
}

func (m MinioWalker) Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error {

	bucket := storageURI.Host
	prefix := strings.TrimLeft(storageURI.Path, "/")
	objectCh := m.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("error minioObject: %w", object.Err)
		}

		addr := fmt.Sprintf("s3://%s/%s", bucket, object.Key)
		ent := ObjectStoreEntry{
			FullKey:     object.Key,
			RelativeKey: strings.TrimPrefix(object.Key, prefix),
			Address:     addr,
			ETag:        object.ETag,
			Mtime:       object.LastModified,
			Size:        object.Size,
		}
		err := walkFn(ent)
		if err != nil {
			return err
		}
		fmt.Println(object)
	}
	return nil
}
