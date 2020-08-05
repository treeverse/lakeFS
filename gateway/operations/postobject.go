package operations

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/serde"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

const (
	CreateMultipartUploadQueryParam   = "uploads"
	CompleteMultipartUploadQueryParam = "uploadId"
)

type PostObject struct{}

func (controller *PostObject) RequiredPermissions(_ *http.Request, repoID, _, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *PostObject) HandleCreateMultipartUpload(o *PathOperation) {
	//var err error
	o.Incr("create_mpu")
	uuidBytes := [16]byte(uuid.New())
	objName := hex.EncodeToString(uuidBytes[:])
	storageClass := StorageClassFromHeader(o.Request.Header)
	opts := block.CreateMultiPartUploadOpts{StorageClass: storageClass}
	uploadID, err := o.BlockStore.CreateMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, o.Request, opts)
	if err != nil {
		o.Log().WithError(err).Error("could not create multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.Cataloger.CreateMultipartUpload(o.Context(), o.Repository.Name, uploadID, o.Path, objName, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not write multipart upload to DB")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(&serde.InitiateMultipartUploadResult{
		Bucket:   o.Repository.Name,
		Key:      o.Path,
		UploadID: uploadID,
	}, http.StatusOK)
}

func trimQuotes(s string) string {
	return strings.Trim(s, "\"")
}

func (controller *PostObject) HandleCompleteMultipartUpload(o *PathOperation) {
	var etag *string
	var size int64
	o.Incr("complete_mpu")
	uploadID := o.Request.URL.Query().Get(CompleteMultipartUploadQueryParam)
	o.AddLogFields(logging.Fields{"upload_id": uploadID})
	multiPart, err := o.Cataloger.GetMultipartUpload(o.Context(), o.Repository.Name, uploadID)
	if err != nil {
		o.Log().WithError(err).Error("could not read multipart record")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	objName := multiPart.PhysicalAddress
	o.AddLogFields(logging.Fields{"physical_address": objName})
	xmlMultipartComplete, err := ioutil.ReadAll(o.Request.Body)
	if err != nil {
		o.Log().WithError(err).Error("could not read request body")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	var MultipartList block.MultipartUploadCompletion
	err = xml.Unmarshal(xmlMultipartComplete, &MultipartList)
	if err != nil {
		o.Log().WithError(err).Error("could not parse multipart XML on complete multipart")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	etag, size, err = o.BlockStore.CompleteMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, uploadID, &MultipartList)
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	ch := trimQuotes(*etag)
	checksum := strings.Split(ch, "-")[0]
	err = o.finishUpload(o.Repository.StorageNamespace, checksum, objName, size)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.Cataloger.DeleteMultipartUpload(o.Context(), o.Repository.Name, uploadID)
	if err != nil {
		o.Log().WithError(err).Warn("could not delete multipart record")
	}

	scheme := httputil.RequestScheme(o.Request)
	location := fmt.Sprintf("%s://%s.%s/%s/%s", scheme, o.Repository, o.FQDN, o.Reference, o.Path)
	o.EncodeResponse(&serde.CompleteMultipartUploadResult{
		Location: location,
		Bucket:   o.Repository.Name,
		Key:      o.Path,
		ETag:     *etag,
	}, http.StatusOK)
}

func (controller *PostObject) Handle(o *PathOperation) {
	// POST is only supported for CreateMultipartUpload/CompleteMultipartUpload
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
	_, mpuCreateParamExist := o.Request.URL.Query()[CreateMultipartUploadQueryParam]
	if mpuCreateParamExist {
		controller.HandleCreateMultipartUpload(o)
		return
	}

	_, mpuCompleteParamExist := o.Request.URL.Query()[CompleteMultipartUploadQueryParam]
	if mpuCompleteParamExist {
		controller.HandleCompleteMultipartUpload(o)
		return
	}

	// otherwise
	o.ResponseWriter.WriteHeader(http.StatusMethodNotAllowed)
}
