package onboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"net/url"
)

type ManifestFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

type S3Manifest struct {
	InventoryBucketArn string         `json:"destinationBucket"`
	Files              []ManifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
}

type InventoryRow struct {
	Error          error
	Bucket         string `parquet:"name=bucket, type=INTERVAL"`
	Key            string `parquet:"name=key, type=INTERVAL"`
	Size           *int64 `parquet:"name=size, type=INT_64"`
	ETag           string `parquet:"name=e_tag, type=INTERVAL"`
	LastModified   int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	IsLatest       bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
}

func FetchManifest(svc s3iface.S3API, manifestURL string) (*S3Manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return nil, err
	}
	var manifest *S3Manifest
	err = json.NewDecoder(output.Body).Decode(&manifest)
	if err != nil {
		return nil, err
	}
	if manifest.Format != "Parquet" {
		return nil, errors.New("currently only parquet inventories are supported. got: " + manifest.Format)
	}
	return manifest, err
}

func FetchInventory(ctx context.Context, svc s3iface.S3API, manifestURL string) (<-chan InventoryRow, error) {
	manifest, err := FetchManifest(svc, manifestURL)
	if err != nil {
		return nil, err
	}
	res := make(chan InventoryRow)
	go func() {
		defer close(res)
		for _, file := range manifest.Files {
			inventoryBucketArn, err := arn.Parse(manifest.InventoryBucketArn)
			if err != nil {
				res <- InventoryRow{Error: fmt.Errorf("failed to parse inventory bucket arn: %v", err)}
				continue
			}
			inventoryBucketName := inventoryBucketArn.Resource
			log.Info(inventoryBucketName + "/" + file.Key)
			pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, inventoryBucketName, file.Key)
			if err != nil {
				res <- InventoryRow{Error: fmt.Errorf("failed to create parquet file reader: %v", err)}
				continue
			}
			pr, err := reader.NewParquetReader(pf, new(InventoryRow), 4)
			if err != nil {
				res <- InventoryRow{Error: fmt.Errorf("failed to create parquet reader: %v", err)}
				continue
			}
			num := int(pr.GetNumRows())
			rows := make([]InventoryRow, num)

			err = pr.Read(&rows)
			if err != nil {
				res <- InventoryRow{Error: fmt.Errorf("failed to read rows from inventory: %v", err)}
				continue
			}
			err = pf.Close()
			if err != nil {

			}
			for _, r := range rows {
				res <- r
			}

		}
	}()
	return res, nil
}
