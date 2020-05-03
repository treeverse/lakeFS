package operations

import (
	"fmt"
	"github.com/treeverse/lakefs/block/s3"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/upload"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/permissions"
)

const (
	CopySourceHeader     = "x-amz-copy-source"
	QueryParamUploadId   = "uploadId"
	QueryParamPartNumber = "partNumber"
)

type PutObject struct{}

func (controller *PutObject) Action(repoId, refId, path string) permissions.Action {
	return permissions.WriteObject(repoId)
}

func (controller *PutObject) HandleCopy(o *PathOperation, copySource string) {
	o.Incr("copy_object")
	// resolve source branch and source path
	copySourceDecoded, err := url.QueryUnescape(copySource)
	if err != nil {
		copySourceDecoded = copySource
	}
	p, err := path.ResolveAbsolutePath(copySourceDecoded)
	if err != nil {
		o.Log().WithError(err).Error("could not parse copy source path")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// validate src and dst are in the same repo
	if !strings.EqualFold(o.Repo.Id, p.Repo) {
		o.Log().WithError(err).Error("cannot copy objects across repos")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	src, err := o.Index.ReadEntryObject(o.Repo.Id, p.Ref, p.Path)
	if err != nil {
		o.Log().WithError(err).Error("could not read copy source")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	src.CreationDate = time.Now() // TODO: move this logic into the Index impl.
	err = o.Index.WriteEntry(o.Repo.Id, o.Ref, o.Path, src)
	if err != nil {
		o.Log().WithError(err).Error("could not write copy destination")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(&serde.CopyObjectResult{
		LastModified: serde.Timestamp(src.CreationDate),
		ETag:         fmt.Sprintf("\"%s\"", src.Checksum),
	}, http.StatusOK)
}

func (controller *PutObject) HandleUploadPart(o *PathOperation) {
	o.Incr("put_mpu_part")
	query := o.Request.URL.Query()
	uploadId := query.Get(QueryParamUploadId)
	partNumberStr := query.Get(QueryParamPartNumber)

	partNumber, err := strconv.ParseInt(partNumberStr, 10, 64)
	if err != nil {
		o.Log().WithError(err).Error("invalid part number")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidPartNumberMarker))
		return
	}

	// handle the upload itself
	adapter := o.BlockStore
	adapterType := adapter.GetAdapterType()
	if adapterType == "s3" {
		s3adapter, _ := adapter.(s3.AdapterInterface)
		resp, err := s3adapter.UploadPart(o.Repo.StorageNamespace, o.Path, o.Request.ContentLength, o.Request.Body, uploadId, partNumber)
		if err != nil {
			o.Log().WithError(err).Error("part " + partNumberStr + "upload failed")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		o.ResponseWriter.WriteHeader(http.StatusOK)
		for k, val := range resp.Header {
			for _, s := range val {
				o.SetHeader(k, s)
			}
		}
	} else {
		blob, err := upload.WriteBlob(o.Index, o.Repo.Id, o.Repo.StorageNamespace, o.Request.Body, o.BlockStore, o.Request.ContentLength)
		if err != nil {
			o.Log().WithError(err).Error("could not write request body to block adapter")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}

		err = o.MultipartManager.UploadPart(o.Repo.Id, o.Path, uploadId, int(partNumber), &model.MultipartUploadPart{
			Blocks:       blob.Blocks,
			Checksum:     blob.Checksum,
			CreationDate: time.Now(),
			Size:         blob.Size,
		})

		if err != nil {
			o.Log().WithError(err).Error("error writing mpu uploaded part")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		o.ResponseWriter.WriteHeader(http.StatusOK)
		o.SetHeader("ETag", fmt.Sprintf("\"%s\"", blob.Checksum))

	}
	// must write the etag back
	// TODO: validate the ETag sent in CompleteMultipartUpload matches the blob for the given part number
	o.Log().WithFields(logging.Fields{
		"upload_id":   uploadId,
		"part_number": partNumber,
	}).Info("multipart upload part done")
}

func (controller *PutObject) Handle(o *PathOperation) {
	// check if this is a copy operation (i.e.https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header

	//validate branch
	_, err := o.Index.GetBranch(o.Repo.Id, o.Ref)
	if err != nil {
		o.Log().WithError(err).Debug("trying to write to invalid branch")
		o.ResponseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	copySource := o.Request.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		controller.HandleCopy(o, copySource)
		return
	}

	query := o.Request.URL.Query()

	// check if this is a multipart upload creation call
	_, hasUploadId := query[QueryParamUploadId]
	if hasUploadId {
		controller.HandleUploadPart(o)
		return
	}

	o.Incr("put_object")
	// handle the upload itself
	blob, err := upload.WriteBlob(o.Index, o.Repo.Id, o.Repo.StorageNamespace, o.Request.Body, o.BlockStore, o.Request.ContentLength)
	if err != nil {
		o.Log().WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	writeTime := time.Now()
	obj := &model.Object{
		Blocks:   blob.Blocks,
		Checksum: blob.Checksum,
		Metadata: nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Size:     blob.Size,
	}

	p := pth.New(o.Path, model.EntryTypeObject)

	entry := &model.Entry{
		Name:         p.BaseName(),
		Address:      ident.Hash(obj),
		EntryType:    model.EntryTypeObject,
		CreationDate: writeTime,
		Size:         blob.Size,
		Checksum:     blob.Checksum,
	}
	err = o.Index.WriteFile(o.Repo.Id, o.Ref, o.Path, entry, obj)
	tookMeta := time.Since(writeTime)

	if err != nil {
		o.Log().WithError(err).Error("could not update metadata")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.Log().WithFields(logging.Fields{
		"took": tookMeta,
	}).Debug("metadata update complete")
	o.SetHeader("ETag", httputil.ETag(obj.Checksum))
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
