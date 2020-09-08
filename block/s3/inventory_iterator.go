package s3

import (
	"errors"

	"github.com/treeverse/lakefs/block"
	inventorys3 "github.com/treeverse/lakefs/inventory/s3"
)

const DefaultReadBatchSize = 100000

var ErrInventoryNotSorted = errors.New("inventory assumed to be sorted but isn't")

type InventoryIterator struct {
	*Inventory
	ReadBatchSize      int
	err                error
	val                *block.InventoryObject
	buffer             []inventorys3.InventoryObject
	inventoryFileIndex int
	numOfRows          int
	nextRowInFile      int
	valIndexInBuffer   int
}

func NewInventoryIterator(inv *Inventory) *InventoryIterator {
	batchSize := DefaultReadBatchSize
	if inv.Manifest.Format == inventorys3.OrcFormatName {
		batchSize = -1
	}
	return &InventoryIterator{
		Inventory:          inv,
		ReadBatchSize:      batchSize,
		inventoryFileIndex: -1,
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
	if it.nextRowInFile >= it.numOfRows {
		// no more files left
		if !it.moveToNextInventoryFile() {
			return false
		}
	}
	pr, err := it.reader.GetFileReader(it.Manifest.Format, it.Manifest.inventoryBucket, it.Manifest.Files[it.inventoryFileIndex].Key)
	if err != nil {
		it.err = err
		return false
	}
	defer func() {
		err = pr.Close()
		if err != nil {
			it.logger.Errorf("failed to close manifest file reader. file=%s, err=%w", it.Manifest.Files[it.inventoryFileIndex].Key, err)
		}
	}()
	if it.numOfRows == -1 {
		it.numOfRows = int(pr.GetNumRows())
	}

	if !it.fillBuffer(pr) { // fill from current manifest file
		return false
	}
	return it.Next()
}

func (it *InventoryIterator) moveToNextInventoryFile() bool {
	if it.inventoryFileIndex == len(it.Manifest.Files)-1 {
		return false
	}
	it.inventoryFileIndex += 1
	it.numOfRows = -1
	it.logger.Debugf("moving to next manifest file: %s", it.Manifest.Files[it.inventoryFileIndex].Key)
	it.nextRowInFile = 0
	it.buffer = nil
	return true
}

func (it *InventoryIterator) fillBuffer(reader inventorys3.FileReader) bool {
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
	it.buffer = make([]inventorys3.InventoryObject, batchSize)
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
