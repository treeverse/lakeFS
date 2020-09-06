package s3

import (
	"errors"

	"github.com/treeverse/lakefs/block"
)

const DefaultReadBatchSize = 100000

var ErrInventoryNotSorted = errors.New("inventory assumed to be sorted but isn't")

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
	ReadBatchSize           int
	err                     error
	val                     *block.InventoryObject
	buffer                  []InventoryObject
	currentInventoryFileIdx int
	currentRowNumber        int
	nextRowInFile           int
	valIndexInBuffer        int
}

func NewInventoryIterator(inv *Inventory) *InventoryIterator {
	batchSize := DefaultReadBatchSize
	if inv.Manifest.Format == OrcFormatName {
		batchSize = -1
	}
	return &InventoryIterator{
		Inventory:               inv,
		ReadBatchSize:           batchSize,
		currentInventoryFileIdx: -1,
	}
}

func (it *InventoryIterator) Next() bool {
	if len(it.Manifest.Files) == 0 {
		// empty manifest
		return false
	}
	val, valIndex := it.nextFromBuffer()
	if val != nil {
		// found the next object in buffer
		it.valIndexInBuffer = valIndex
		if it.shouldSort && it.val != nil && val.Key < it.val.Key {
			it.err = ErrInventoryNotSorted
			return false
		}
		it.val = val
		return true
	}
	// value not found in buffer, need to reload the buffer
	it.valIndexInBuffer = -1
	if it.nextRowInFile >= it.currentRowNumber {
		// no more files left
		if !it.moveToNextInventoryFile() {
			return false
		}
	}
	pr, err := it.reader.GetInventoryFileReader(it.Manifest, it.Manifest.Files[it.currentInventoryFileIdx].Key)
	if err != nil {
		it.err = err
		return false
	}
	defer func() {
		err = pr.Close()
		if err != nil {
			it.logger.Errorf("failed to close manifest file reader. file=%s", it.Manifest.Files[it.currentInventoryFileIdx].Key)
		}
	}()
	if it.currentRowNumber == -1 {
		it.currentRowNumber = int(pr.GetNumRows())
	}

	if !it.fillBuffer(pr) { // fill from current manifest file
		return false
	}
	return it.Next()
}

func (it *InventoryIterator) moveToNextInventoryFile() bool {
	if it.currentInventoryFileIdx == len(it.Manifest.Files)-1 {
		return false
	}
	it.currentInventoryFileIdx += 1
	it.currentRowNumber = -1
	it.logger.Debugf("moving to next manifest file: %s", it.Manifest.Files[it.currentInventoryFileIdx].Key)
	it.nextRowInFile = 0
	it.buffer = nil
	return true
}

func (it *InventoryIterator) fillBuffer(reader InventoryFileReader) bool {
	it.logger.Debug("start reading rows from inventory to buffer")
	// skip the rows that have already been read:
	err := reader.SkipRows(int64(it.nextRowInFile))
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
	it.nextRowInFile += len(it.buffer)
	return true
}

func (it *InventoryIterator) nextFromBuffer() (*block.InventoryObject, int) {
	for i := it.valIndexInBuffer + 1; i < len(it.buffer); i++ {
		obj := it.buffer[i]
		if (obj.IsLatest != nil && !*obj.IsLatest) ||
			(obj.IsDeleteMarker != nil && *obj.IsDeleteMarker) {
			continue
		}
		res := block.InventoryObject{
			Bucket:          obj.Bucket,
			Key:             obj.Key,
			PhysicalAddress: obj.GetPhysicalAddress(),
		}
		if obj.Size != nil {
			res.Size = *obj.Size
		}
		if obj.LastModified != nil {
			res.LastModified = *obj.LastModified
		}
		if obj.Checksum != nil {
			res.Checksum = *obj.Checksum
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
