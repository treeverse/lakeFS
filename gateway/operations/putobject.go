package operations

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"treeverse-lake/block"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/path"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/gateway/serde"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"

	log "github.com/sirupsen/logrus"
)

const (
	// size of physical object to store in the underlying block adapter
	// TODO: should probably be a configuration parameter
	ObjectBlockSize = 128 * 1024 * 1024

	CopySourceHeader = "x-amz-copy-source"

	QueryParamUploadId   = "uploadId"
	QueryParamPartNumber = "partNumber"
)

type PutObject struct{}

func (controller *PutObject) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *PutObject) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *PutObject) HandleCopy(o *PathOperation, copySource string) {
	// resolve source branch and source path
	p, err := path.ResolveAbsolutePath(copySource)
	if err != nil {
		o.Log().WithError(err).Error("could not parse copy source path")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// validate src and dst are in the same repo
	if !strings.EqualFold(o.Repo, p.Repo) {
		o.Log().WithError(err).Error("cannot copy objects across repos")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}

	// update metadata to refer to the source hash in the destination workspace
	src, err := o.Index.ReadObject(o.Repo, p.Refspec, p.Path)
	if err != nil {
		o.Log().WithError(err).WithFields(log.Fields{
			"repo":   o.Repo,
			"branch": p.Refspec,
			"path":   p.Path,
		}).Error("could not read copy source")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopySource))
		return
	}
	// write this object to workspace
	err = o.Index.WriteObject(o.Repo, o.Branch, o.Path, src)
	if err != nil {
		o.Log().WithError(err).Error("could not write copy destination")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInvalidCopyDest))
		return
	}

	o.EncodeResponse(&serde.CopyObjectResult{
		LastModified: serde.Timestamp(src.GetTimestamp()),
		ETag:         fmt.Sprintf("\"%s\"", ident.Hash(src)),
	}, http.StatusOK)
}

func (controller *PutObject) HandleCreateMultipartUpload(o *PathOperation) {
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
	blob, size, err := ReadBlob(o.Request.Body, o.BlockStore)
	if err != nil {
		o.Log().WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	err = o.MultipartManager.UploadPart(o.Repo, o.Path, uploadId, int(partNumber), &model.MultipartUploadPart{
		Blob:      blob,
		Timestamp: time.Now().Unix(),
		Size:      size,
	})

	if err != nil {
		o.Log().WithError(err).Error("error writing mpu uploaded part")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// must write the etag back
	// TODO: validate the ETag sent in CompleteMultipartUpload matches the blob for the given part number
	o.ResponseWriter.Header().Set("ETag", fmt.Sprintf("\"%s\"", ident.Hash(blob)))
	o.ResponseWriter.WriteHeader(http.StatusOK)

	o.Log().WithFields(log.Fields{
		"upload_id":   uploadId,
		"part_number": partNumber,
	}).Info("MULTI PART PART DONE!")
}

func ReadBlob(body io.Reader, adapter block.Adapter) (*model.Blob, int64, error) {
	// handle the upload itself
	blocks := make([]*model.Block, 0)
	var totalSize int64
	var done bool
	for !done {
		buf := make([]byte, ObjectBlockSize)
		n, err := body.Read(buf)

		// unexpected error
		if err != nil && err != io.EOF {
			return nil, 0, err
		}

		// body is completely drained and we read nothing
		if err == io.EOF && n == 0 {
			break // nothing left to do, we read the whole thing
		}

		// body is completely drained and we read the remainder
		if err == io.EOF {
			done = true
		}

		// write a block
		blockAddr := ident.Bytes(buf[:n]) // content based addressing happens here
		w, err := adapter.Put(blockAddr)
		if err != nil {
			return nil, 0, err
		}
		_, err = w.Write(buf[:n])
		_ = w.Close()
		if err != nil {
			return nil, 0, err
		}
		blocks = append(blocks, &model.Block{
			Address: blockAddr,
			Size:    int64(n),
		})
		totalSize += int64(n)

		if done {
			break
		}
	}
	return &model.Blob{Blocks: blocks}, totalSize, nil
}

func (controller *PutObject) Handle(o *PathOperation) {
	// check if this is a copy operation (i.e.https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
	// A copy operation is identified by the existence of an "x-amz-copy-source" header
	copySource := o.Request.Header.Get(CopySourceHeader)
	if len(copySource) > 0 {
		controller.HandleCopy(o, copySource)
		return
	}

	query := o.Request.URL.Query()

	_, hasUploadId := query[QueryParamUploadId]
	if hasUploadId {
		controller.HandleCreateMultipartUpload(o)
		return
	}

	// handle the upload itself
	blob, size, err := ReadBlob(o.Request.Body, o.BlockStore)
	if err != nil {
		o.Log().WithError(err).Error("could not write request body to block adapter")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write metadata
	writeTime := time.Now()
	err = o.Index.WriteObject(o.Repo, o.Branch, o.Path, &model.Object{
		Blob:      blob,
		Metadata:  nil, // TODO: Read whatever metadata came from the request headers/params and add here
		Timestamp: writeTime.Unix(),
		Size:      size,
	})
	tookMeta := time.Since(writeTime)

	if err != nil {
		o.Log().WithError(err).Error("could not update metadata")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.Log().WithFields(log.Fields{
		"took":   tookMeta,
		"repo":   o.Repo,
		"branch": o.Branch,
		"path":   o.Path,
	}).Trace("metadata update complete")
	o.ResponseWriter.WriteHeader(http.StatusCreated)
}
