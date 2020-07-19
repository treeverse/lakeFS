package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
)

type InventoryIterator struct {
	s3                     s3iface.S3API
	ctx                    context.Context
	manifest               manifest
	inventoryBucket        string
	value                  block.InventoryObject
	err                    error
	currentManifestFileIdx int
	currentChannel         <-chan ParquetInventoryObject
	getParquetReader       parquetReaderGetter
	ReadBatchSize          int
}

func (i *InventoryIterator) Next() bool {
	for {
		var errs <-chan error
		if i.currentChannel == nil {
			i.currentManifestFileIdx += 1
			if i.currentManifestFileIdx >= len(i.manifest.Files) {
				return false
			}
			pr, err := i.getParquetReader(i.ctx, i.s3, i.inventoryBucket, i.manifest.Files[i.currentManifestFileIdx].Key)
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
