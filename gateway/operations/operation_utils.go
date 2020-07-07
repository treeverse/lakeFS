package operations

import (
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
)

func (o *PathOperation) finishUpload(blockAdapter block.Adapter, storageNamespace, checksum, physicalAddress string, size int64) error {
	// write metadata
	writeTime := time.Now()
	entry := catalog.Entry{
		Path:            o.Path,
		PhysicalAddress: physicalAddress,
		Checksum:        checksum,
		Metadata:        nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Size:            size,
		CreationDate:    writeTime,
	}

	dedupCh := make(chan *catalog.DedupResult)
	err := o.Cataloger.CreateEntryDedup(o.Context(), o.Repository.Name, o.Reference, entry, catalog.DedupParams{
		ID:               checksum,
		Ch:               dedupCh,
		StorageNamespace: storageNamespace,
	})
	if err != nil {
		o.Log().WithError(err).Error("could not update metadata")
		return err
	}
	go func() {
		dedupResult := <-dedupCh
		if dedupResult.NewPhysicalAddress != "" {
			_ = blockAdapter.Remove(block.ObjectPointer{
				StorageNamespace: dedupResult.StorageNamespace,
				Identifier:       dedupResult.Entry.PhysicalAddress,
			})
		}
	}()
	tookMeta := time.Since(writeTime)
	o.Log().WithFields(logging.Fields{
		"took": tookMeta,
	}).Debug("metadata update complete")
	return nil
}
