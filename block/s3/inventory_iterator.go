package s3

import (
	"github.com/treeverse/lakefs/block"
)

const DefaultReadBatchSize = 100000

type InventoryObject struct {
	Bucket         string  `parquet:"name=bucket, type=UTF8"`
	Key            string  `parquet:"name=key, type=UTF8"`
	IsLatest       *bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker *bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
	Size           *int64  `parquet:"name=size, type=INT_64"`
	LastModified   *int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	Checksum       *string `parquet:"name=e_tag, type=UTF8"`
}

func (o *InventoryObject) GetPhysicalAddress() string {
	return "s3://" + o.Bucket + "/" + o.Key
}

type InventoryIterator struct {
	*Inventory
	ReadBatchSize          int
	err                    error
	val                    *block.InventoryObject
	buffer                 []InventoryObject
	currentManifestFileIdx int
	nextRowInParquet       int
	valIndexInBuffer       int
}

func NewInventoryIterator(inv *Inventory) *InventoryIterator {
	batchSize := DefaultReadBatchSize
	if inv.Manifest.Format == "ORC" {
		batchSize = -1
	}
	return &InventoryIterator{
		Inventory:     inv,
		ReadBatchSize: batchSize,
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
			it.val = val
			return true
		}
		// value not found in buffer, need to reload the buffer
		it.valIndexInBuffer = -1
		// if needed, try to move on to the next manifest file:
		file := it.Manifest.Files[it.currentManifestFileIdx]
		pr, err := it.inventoryReader.GetReader(it.ctx, *it.Manifest, file.Key)
		if err != nil {
			it.err = err
			return false
		}
		if it.nextRowInParquet >= int(pr.GetNumRows()) {
			// no more files left
			if it.moveToNextManifestFile() {
				err = pr.Close()
				if err != nil {
					it.logger.Errorf("failed to close manifest file reader: %v", err)
				}
			} else {
				return false
			}
		}
		file = it.Manifest.Files[it.currentManifestFileIdx]
		if err != nil {
			it.err = err
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
	it.logger.Info("moving to next manifest file")
	it.currentManifestFileIdx += 1
	it.nextRowInParquet = 0
	it.buffer = nil
	return true
}

func (it *InventoryIterator) fillBuffer() bool {
	it.logger.Info("start reading rows from inventory to buffer")
	file := &it.Manifest.Files[it.currentManifestFileIdx]
	reader, err := it.inventoryReader.GetReader(it.ctx, *it.Manifest, file.Key)
	if err != nil {
		it.err = err
		return false
	}
	// skip the rows that have already been read:
	err = reader.SkipRows(int64(it.nextRowInParquet))
	if err != nil {
		it.err = err
		return false
	}
	batchSize := it.ReadBatchSize
	if batchSize == -1 {
		batchSize = int(reader.GetNumRows())
	}
	it.buffer = make([]InventoryObject, batchSize)

	// read a batch of rows according to the batch size:
	err = reader.Read(&it.buffer)
	if err != nil {
		it.err = err
		return false
	}
	it.nextRowInParquet += len(it.buffer)
	return true
}

func (it *InventoryIterator) nextFromBuffer() (*block.InventoryObject, int) {
	for i := it.valIndexInBuffer + 1; i < len(it.buffer); i++ {
		parquetObj := it.buffer[i]
		if (parquetObj.IsLatest != nil && !*parquetObj.IsLatest) ||
			(parquetObj.IsDeleteMarker != nil && *parquetObj.IsDeleteMarker) {
			continue
		}
		res := block.InventoryObject{
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
	return it.val
}
