package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

const DefaultReadBatchSize = 100000

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
	err                    error
	currentManifestFileIdx int
	rowsPerFile            []int
	nextRowIdx             int
	buffer                 []ParquetInventoryObject
	valueIdx               int
	val                    block.InventoryObject
	rowsRead               int
}

type ParquetReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
	SkipRows(int64) error
	Close()
}

type ParquetReaderBundle struct {
	reader.ParquetReader
	file source.ParquetFile
}

func (p *ParquetReaderBundle) Close() {
	p.ReadStop()
	_ = p.file.Close()
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
func (i *InventoryIterator) loadRowNums() bool {
	if i.rowsPerFile == nil {
		i.rowsPerFile = make([]int, len(i.manifest.Files))
		for j := range i.manifest.Files {
			pr, err := i.GetParquetReader(i.ctx, i.s3, i.inventoryBucket, i.manifest.Files[j].Key)
			if err != nil {
				i.err = err
				return false
			}
			i.rowsPerFile[j] = int(pr.GetNumRows())
			pr.Close()
		}
	}
	return true
}

func (i *InventoryIterator) Next() bool {
	if !i.loadRowNums() {
		return false
	}
	for {
		if i.nextFromBuffer() {
			return true
		}
		if !i.tryLoadBuffer() {
			return false
		}
	}
}

func (i *InventoryIterator) tryLoadBuffer() bool {
	if i.currentManifestFileIdx < 0 || i.nextRowIdx >= i.rowsPerFile[i.currentManifestFileIdx] {
		i.currentManifestFileIdx += 1
		i.nextRowIdx = 0
		i.buffer = nil
	}
	if i.buffer == nil || i.valueIdx+1 >= len(i.buffer) {
		i.buffer = make([]ParquetInventoryObject, i.ReadBatchSize)
		i.valueIdx = -1
	}
	if i.currentManifestFileIdx >= len(i.manifest.Files) {
		return false
	}
	pr, err := i.GetParquetReader(i.ctx, i.s3, i.inventoryBucket, i.manifest.Files[i.currentManifestFileIdx].Key)
	if err != nil {
		i.err = err
		return false
	}
	defer pr.Close()
	err = pr.SkipRows(int64(i.nextRowIdx))
	if err != nil {
		i.err = err
		return false
	}
	err = pr.Read(&i.buffer)
	if err != nil {
		i.err = err
		return false
	}
	i.nextRowIdx += len(i.buffer)
	return true
}

func (i *InventoryIterator) nextFromBuffer() bool {
	var res block.InventoryObject
	for i.valueIdx = i.valueIdx + 1; i.valueIdx < len(i.buffer); i.valueIdx++ {
		parquetObj := i.buffer[i.valueIdx]
		if (parquetObj.IsLatest == nil || *parquetObj.IsLatest) &&
			(parquetObj.IsDeleteMarker == nil || !*parquetObj.IsDeleteMarker) {
			res = block.InventoryObject{
				Bucket:          parquetObj.Bucket,
				Key:             parquetObj.Key,
				PhysicalAddress: parquetObj.GetPhysicalAddress(),
			}
			if parquetObj.Size != nil {
				res.Size = *parquetObj.Size
			}
			if parquetObj.LastModified != nil {
				res.LastModified = *parquetObj.LastModified
			}
			if parquetObj.Checksum != nil {
				res.Checksum = *parquetObj.Checksum
			}
			i.val = res
			return true
		}
	}
	return false
}

func (i *InventoryIterator) Err() error {
	return i.err
}

func (i *InventoryIterator) Get() *block.InventoryObject {
	return &i.val
}

func getParquetReader(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, manifestFileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	return &ParquetReaderBundle{*pr, pf}, nil
}
