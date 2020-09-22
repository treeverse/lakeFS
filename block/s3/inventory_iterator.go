package s3

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gosuri/uiprogress"

	"github.com/treeverse/lakefs/block"
	inventorys3 "github.com/treeverse/lakefs/inventory/s3"
)

var ErrInventoryNotSorted = errors.New("got unsorted s3 inventory")

type InventoryIterator struct {
	*Inventory
	err                error
	val                *block.InventoryObject
	buffer             []inventorys3.InventoryObject
	inventoryFileIndex int
	valIndexInBuffer   int
	fileProgress       *uiprogress.Bar
	objectProgress     *uiprogress.Bar
}

func NewInventoryIterator(inv *Inventory) *InventoryIterator {
	creationTimestamp, err := strconv.ParseInt(inv.Manifest.CreationTimestamp, 10, 64)
	if err != nil {
		inv.logger.Errorf("failed to get creation timestamp from manifest")
		creationTimestamp = 0
	}
	t := time.Unix(creationTimestamp/int64(time.Second/time.Millisecond), 0)
	fileProgress := uiprogress.AddBar(len(inv.Manifest.Files))
	prefix := fmt.Sprintf("inventory %s", t.Format("2006-01-02"))
	fileProgress.PrependFunc(func(b *uiprogress.Bar) string {
		return prefix
	})
	fileProgress.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("%d/%d inventory files", b.Current(), b.Total)
	})
	objectProgress := uiprogress.AddBar(1)
	objectProgress.PrependFunc(func(b *uiprogress.Bar) string {
		return strings.Repeat(" ", len(prefix))
	})
	objectProgress.AppendFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("%d/%d rows in file", b.Current(), b.Total)
	})
	return &InventoryIterator{
		Inventory:          inv,
		inventoryFileIndex: -1,
		fileProgress:       fileProgress,
		objectProgress:     objectProgress,
	}
}

func (it *InventoryIterator) Next() bool {
	for {
		if len(it.Manifest.Files) == 0 {
			// empty manifest
			return false
		}
		val := it.nextFromBuffer()
		if val != nil {
			// validate element order
			if it.shouldSort && it.val != nil && val.Key < it.val.Key {
				it.err = ErrInventoryNotSorted
				return false
			}
			it.objectProgress.Incr()
			it.val = val
			return true
		}
		// value not found in buffer, need to reload the buffer
		it.valIndexInBuffer = -1
		if !it.moveToNextInventoryFile() {
			// no more files left
			return false
		}
		if !it.fillBuffer() {
			return false
		}
	}
}

func (it *InventoryIterator) moveToNextInventoryFile() bool {
	if it.inventoryFileIndex == len(it.Manifest.Files)-1 {
		return false
	}
	it.inventoryFileIndex += 1
	it.fileProgress.Incr()
	it.logger.Debugf("moving to next manifest file: %s", it.Manifest.Files[it.inventoryFileIndex].Key)
	it.buffer = nil
	return true
}

func (it *InventoryIterator) fillBuffer() bool {
	it.logger.Debug("start reading rows from inventory to buffer")
	rdr, err := it.reader.GetFileReader(it.Manifest.Format, it.Manifest.inventoryBucket, it.Manifest.Files[it.inventoryFileIndex].Key)
	if err != nil {
		it.err = err
		return false
	}
	it.objectProgress.Total = int(rdr.GetNumRows())
	_ = it.objectProgress.Set(0)
	defer func() {
		err = rdr.Close()
		if err != nil {
			it.logger.Errorf("failed to close manifest file reader. file=%s, err=%w", it.Manifest.Files[it.inventoryFileIndex].Key, err)
		}
	}()
	it.buffer = make([]inventorys3.InventoryObject, rdr.GetNumRows())
	err = rdr.Read(&it.buffer)
	if err != nil {
		it.err = err
		return false
	}
	return true
}

func (it *InventoryIterator) nextFromBuffer() *block.InventoryObject {
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
		if obj.LastModifiedMillis != nil {
			res.LastModified = time.Unix(*obj.LastModifiedMillis/int64(time.Second/time.Millisecond), 0)
		}
		if obj.Checksum != nil {
			res.Checksum = *obj.Checksum
		}
		it.valIndexInBuffer = i
		return &res
	}
	return nil
}

func (it *InventoryIterator) Err() error {
	return it.err
}

func (it *InventoryIterator) Get() *block.InventoryObject {
	return it.val
}
