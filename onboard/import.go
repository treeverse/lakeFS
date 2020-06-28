package onboard

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/catalog"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"net/url"
	"time"
)

const LauncherBranchName = "launcher"

type ManifestFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

type S3Manifest struct {
	InventoryBucketArn string         `json:"destinationBucket"`
	Files              []ManifestFile `json:"files"`
}

type InventoryRow struct {
	Error  error
	Bucket string `parquet:"name=bucket, type=INTERVAL"`
	Key    string `parquet:"name=key, type=INTERVAL"`
	Size   *int64 `parquet:"name=size, type=INT_64"`
}

func fetchManifest(ctx context.Context, svc *s3.S3, manifestURL string) (<-chan InventoryRow, error) {
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

func Import(ctx context.Context, svc *s3.S3, cataloger catalog.Cataloger, manifestURL string, repository string) error {

	repo, err := cataloger.GetRepository(ctx, repository)
	if err != nil {
		return err
	}

	err = cataloger.CreateBranch(ctx, repository, LauncherBranchName, repo.DefaultBranch)

	if err != nil {
		//return err
	}
	rowsChan, err := fetchManifest(ctx, svc, manifestURL)
	if err != nil {
		return err
	}

	for row := range rowsChan {
		if row.Error != nil {
			log.Errorf("failed to read row from inventory: %v", row.Error)
			continue
		}
		entry := catalog.Entry{
			Path:            row.Key,
			PhysicalAddress: "s3://" + row.Bucket + "/" + row.Key,
			CreationDate:    time.Time{},
			Size:            *row.Size,
			Checksum:        "",
			Metadata:        nil,
		}
		err = cataloger.CreateEntry(ctx, repository, LauncherBranchName, entry)
		if err != nil {
			log.Errorf("failed to create entry %s (%v)", row.Key, err)
		}
	}
	return nil
}
