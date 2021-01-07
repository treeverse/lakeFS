package operations

import (
	"net/http"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/logging"
)

func (o *PathOperation) finishUpload(req *http.Request, storageNamespace, checksum, physicalAddress string, size int64) error {
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

	err := o.Cataloger.CreateEntry(req.Context(), o.Repository.Name, o.Reference, entry,
		catalog.CreateEntryParams{
			Dedup: catalog.DedupParams{
				ID:               checksum,
				StorageNamespace: storageNamespace,
			},
		})
	if err != nil {
		o.Log(req).WithError(err).Error("could not update metadata")
		return err
	}
	tookMeta := time.Since(writeTime)
	o.Log(req).WithFields(logging.Fields{
		"took": tookMeta,
	}).Debug("metadata update complete")
	return nil
}
