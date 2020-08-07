package s3

import (
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
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
	ReadBatchSize          int
	err                    error
	val                    block.InventoryObject
	buffer                 []ParquetInventoryObject
	currentManifestFileIdx int
	nextRowInParquet       int
	valIndexInBuffer       int
}

func NewInventoryIterator(inv *Inventory) *InventoryIterator {
	return &InventoryIterator{
		Inventory:     inv,
		ReadBatchSize: DefaultReadBatchSize,
	}
}

func (it *InventoryIterator) Next() bool {
	if len(it.Manifest.Files) == 0 {
		// empty manifest
		return false
	}
	for {
		val, valIndex := it.nextFromBuffer()
		if val != nil {
			// found the next object in buffer
			it.valIndexInBuffer = valIndex
			it.val = *val
			return true
		}
		// value not found in buffer, need to reload the buffer
		it.valIndexInBuffer = -1
		// if needed, try to move on to the next manifest file:
		file := it.Manifest.Files[it.currentManifestFileIdx]
		if it.nextRowInParquet >= file.numRows && !it.moveToNextManifestFile() {
			// no more files left
			return false
		}
		if !it.fillBuffer() { // fill from current manifest file
			return false
		}
	}
}

func (it *InventoryIterator) moveToNextManifestFile() bool {
	if it.currentManifestFileIdx == len(it.Manifest.Files)-1 {
		return false
	}
	it.currentManifestFileIdx += 1
	it.nextRowInParquet = 0
	it.buffer = nil
	return true
}

func (it *InventoryIterator) fillBuffer() bool {
	key := it.Manifest.Files[it.currentManifestFileIdx].Key
	pr, closeReader, err := it.getParquetReader(it.ctx, it.S3, it.Manifest.inventoryBucket, key)
	if err != nil {
		it.err = err
		return false
	}
	defer func() {
		err = closeReader()
		if err != nil {
			it.logger.WithFields(logging.Fields{"bucket": it.Manifest.inventoryBucket, "key": key}).
				Error("failed to close parquet reader after filling buffer")
		}
	}()
	// skip the rows that have already been read:
	err = pr.SkipRows(int64(it.nextRowInParquet))
	if err != nil {
		it.err = err
		return false
	}
	it.buffer = make([]ParquetInventoryObject, it.ReadBatchSize)
	// read a batch of rows according to the batch size:
	err = pr.Read(&it.buffer)
	if err != nil {
		it.err = err
		return false
	}
	it.nextRowInParquet += len(it.buffer)
	return true
}

func (it *InventoryIterator) nextFromBuffer() (*block.InventoryObject, int) {
	var res block.InventoryObject
	for i := it.valIndexInBuffer + 1; i < len(it.buffer); i++ {
		parquetObj := it.buffer[i]
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
		return &res, i
	}
	return nil, -1
}

func (it *InventoryIterator) Err() error {
	return it.err
}

func (it *InventoryIterator) Get() *block.InventoryObject {
	return &it.val
}
