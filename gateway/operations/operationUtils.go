package operations

import (
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/logging"
	"time"
)

func (o *PathOperation) finishUpload(checksum, physicalAddress string, size int64) error {
	// write metadata

	writeTime := time.Now()
	obj := &model.Object{
		PhysicalAddress: physicalAddress,
		Checksum:        checksum,
		Metadata:        nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Size:            size,
	}

	p := pth.New(o.Path, model.EntryTypeObject)

	entry := &model.Entry{
		Name:         p.BaseName(),
		Address:      ident.Hash(obj),
		EntryType:    model.EntryTypeObject,
		CreationDate: writeTime,
		Size:         size,
		Checksum:     checksum,
	}
	err := o.Index.WriteFile(o.Repo.Id, o.Ref, o.Path, entry, obj)
	tookMeta := time.Since(writeTime)

	if err != nil {
		o.Log().WithError(err).Error("could not update metadata")
		return err
	}
	o.Log().WithFields(logging.Fields{
		"took": tookMeta,
	}).Debug("metadata update complete")
	return nil

}
