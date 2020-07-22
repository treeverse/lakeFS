package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetInventoryObject struct {
	Bucket         string  `parquet:"name=bucket, type=UTF8"`
	Key            string  `parquet:"name=key, type=UTF8"`
	IsLatest       *bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker *bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
	Size           *int64  `parquet:"name=size, type=INT_64"`
	LastModified   *int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	Checksum       *string `parquet:"name=e_tag, type=UTF8"`
}

func (o *ParquetInventoryObject) GetPhysicalAddress() string {
	return "s3://" + o.Bucket + "/" + o.Key
}

type parquetReaderGetter func(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, error)

type InventoryIterator struct {
	iteratorState
	GetParquetReader parquetReaderGetter
	ReadBatchSize    int
	s3               s3iface.S3API
	ctx              context.Context
	manifest         manifest
	inventoryBucket  string
}

type iteratorState struct {
	value                  block.InventoryObject
	err                    error
	currentManifestFileIdx int
	currentChannel         <-chan ParquetInventoryObject
}

type ParquetReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
}

func NewInventoryIterator(s3 s3iface.S3API, ctx context.Context, manifest manifest, inventoryBucket string) *InventoryIterator {
	return &InventoryIterator{
		s3:               s3,
		ctx:              ctx,
		manifest:         manifest,
		inventoryBucket:  inventoryBucket,
		GetParquetReader: getParquetReader,
		ReadBatchSize:    DefaultReadBatchSize,
		iteratorState:    iteratorState{currentManifestFileIdx: -1},
	}
}

func (i *InventoryIterator) Next() bool {
	for {
		var errs <-chan error
		if i.currentChannel == nil {
			i.currentManifestFileIdx += 1
			if i.currentManifestFileIdx >= len(i.manifest.Files) {
				return false
			}
			pr, err := i.GetParquetReader(i.ctx, i.s3, i.inventoryBucket, i.manifest.Files[i.currentManifestFileIdx].Key)
			if err != nil {
				i.err = err
				return false
			}
			i.currentChannel, errs = i.getRowChannel(pr)
		}
		parquetObj, ok := <-i.currentChannel
		if !ok {
			select {
			case err, ok := <-errs:
				if ok {
					i.err = err
					return false
				}
			default:
			}
			i.currentChannel = nil
			continue
		}
		if (parquetObj.IsLatest == nil || *parquetObj.IsLatest) &&
			(parquetObj.IsDeleteMarker == nil || !*parquetObj.IsDeleteMarker) {
			i.value = block.InventoryObject{
				Bucket:          parquetObj.Bucket,
				Key:             parquetObj.Key,
				PhysicalAddress: parquetObj.GetPhysicalAddress(),
			}
			if parquetObj.Size != nil {
				i.value.Size = *parquetObj.Size
			}
			if parquetObj.LastModified != nil {
				i.value.LastModified = *parquetObj.LastModified
			}
			if parquetObj.Checksum != nil {
				i.value.Checksum = *parquetObj.Checksum
			}
			return true
		}
	}
}

func (i *InventoryIterator) Err() error {
	return i.err
}

func (i *InventoryIterator) Get() *block.InventoryObject {
	return &i.value
}

func (i *InventoryIterator) getRowChannel(pr ParquetReader) (<-chan ParquetInventoryObject, <-chan error) {
	num := int(pr.GetNumRows())
	out := make(chan ParquetInventoryObject)
	errs := make(chan error)
	go func() {
		defer close(errs)
		batchSize := i.ReadBatchSize
		if batchSize == 0 {
			batchSize = DefaultReadBatchSize
		}
		rawInventoryObjects := make([]ParquetInventoryObject, batchSize)
		for i := 0; i < num; i += batchSize {
			err := pr.Read(&rawInventoryObjects)
			if err != nil {
				close(out)
				errs <- err
				return
			}
			for _, o := range rawInventoryObjects {
				out <- o
			}
		}
		close(out)
	}()
	return out, errs
}

func getParquetReader(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, manifestFileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	defer func() {
		_ = pf.Close()
	}()
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	return pr, nil
}
