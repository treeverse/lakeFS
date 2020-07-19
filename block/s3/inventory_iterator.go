package s3

import (
	"context"
	"github.com/treeverse/lakefs/block"
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

type InventoryIterator struct {
	*Inventory
	iteratorState
	ctx             context.Context
	ReadBatchSize   int
	inventoryBucket string
}

type iteratorState struct {
	err    error
	val    block.InventoryObject
	buffer []ParquetInventoryObject

	currentManifestFileIdx int
	nextRowInParquet       int
	rowsPerFile            []int
	valIndexInBuffer       int
}

func NewInventoryIterator(ctx context.Context, inv *Inventory, invBucket string) (*InventoryIterator, error) {
	res := &InventoryIterator{
		ctx:             ctx,
		Inventory:       inv,
		ReadBatchSize:   DefaultReadBatchSize,
		inventoryBucket: invBucket,
	}
	res.rowsPerFile = make([]int, len(res.Manifest.Files))
	for j := range res.Manifest.Files {
		pr, closeReader, err := res.getParquetReader(res.ctx, res.S3, res.inventoryBucket, res.Manifest.Files[j].Key)
		if err != nil {
			return nil, err
		}
		res.rowsPerFile[j] = int(pr.GetNumRows())
		closeReader()
	}
	return res, nil
}

func (i *InventoryIterator) Next() bool {
	if len(i.rowsPerFile) == 0 {
		// empty manifest
		return false
	}
	for {
		val, valIndex := i.nextFromBuffer()
		if val != nil {
			// found the next object in buffer
			i.valIndexInBuffer = valIndex
			i.val = *val
			return true
		}
		// value not found in buffer, need to reload the buffer
		i.valIndexInBuffer = -1
		// if needed, try to move on to the next manifest file:
		if i.nextRowInParquet >= i.rowsPerFile[i.currentManifestFileIdx] && !i.moveToNextManifestFile() {
			// no more files left
			return false
		}
		if !i.fillBuffer() { // fill from current manifest file
			return false
		}
	}
}

func (i *InventoryIterator) moveToNextManifestFile() bool {
	if i.currentManifestFileIdx == len(i.Manifest.Files)-1 {
		return false
	}
	i.currentManifestFileIdx += 1
	i.nextRowInParquet = 0
	i.buffer = nil
	return true
}

func (i *InventoryIterator) fillBuffer() bool {
	pr, closeReader, err := i.getParquetReader(i.ctx, i.S3, i.inventoryBucket, i.Manifest.Files[i.currentManifestFileIdx].Key)
	if err != nil {
		i.err = err
		return false
	}
	defer closeReader()
	// skip the rows that have already been read:
	err = pr.SkipRows(int64(i.nextRowInParquet))
	if err != nil {
		i.err = err
		return false
	}
	i.buffer = make([]ParquetInventoryObject, i.ReadBatchSize)
	// read a batch of rows according to the batch size:
	err = pr.Read(&i.buffer)
	if err != nil {
		i.err = err
		return false
	}
	i.nextRowInParquet += len(i.buffer)
	return true
}

func (i *InventoryIterator) nextFromBuffer() (*block.InventoryObject, int) {
	var res block.InventoryObject
	for j := i.valIndexInBuffer + 1; j < len(i.buffer); j++ {
		parquetObj := i.buffer[j]
		if (parquetObj.IsLatest != nil && !*parquetObj.IsLatest) ||
			(parquetObj.IsDeleteMarker != nil && *parquetObj.IsDeleteMarker) {
			continue
		}
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
		return &res, j
	}
	return nil, -1
}

func (i *InventoryIterator) Err() error {
	return i.err
}

func (i *InventoryIterator) Get() *block.InventoryObject {
	return &i.val
}
