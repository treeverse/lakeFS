package operations

import (
	"net/http"
	"time"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
)

func (o *PathOperation) finishUpload(req *http.Request, checksum, physicalAddress string, size int64) error {
	// write metadata
	writeTime := time.Now()
	entry := catalog.DBEntry{
		Path:            o.Path,
		PhysicalAddress: physicalAddress,
		Checksum:        checksum,
		Metadata:        nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Size:            size,
		CreationDate:    writeTime,
	}

	err := o.Catalog.CreateEntry(req.Context(), o.Repository.Name, o.Reference, entry)
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
