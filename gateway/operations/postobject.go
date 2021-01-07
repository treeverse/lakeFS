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
	"github.com/treeverse/lakefs/gateway/path"
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

func (controller *PostObject) HandleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, o *PathOperation) {
	o.Incr("create_mpu")
	uuidBytes := [16]byte(uuid.New())
	objName := hex.EncodeToString(uuidBytes[:])
	storageClass := StorageClassFromHeader(r.Header)
	opts := block.CreateMultiPartUploadOpts{StorageClass: storageClass}
	uploadID, err := o.BlockStore.CreateMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, r, opts)
	if err != nil {
		o.Log(r).WithError(err).Error("could not create multipart upload")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.MultipartsTracker.Create(o.Context(r), uploadID, o.Path, objName, time.Now())
	if err != nil {
		o.Log(r).WithError(err).Error("could not write multipart upload to DB")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(w, r, &serde.InitiateMultipartUploadResult{
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		UploadID: uploadID,
	}, http.StatusOK)
}

func trimQuotes(s string) string {
	return strings.Trim(s, "\"")
}

func (controller *PostObject) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, o *PathOperation) {
	var etag *string
	var size int64
	o.Incr("complete_mpu")
	uploadID := r.URL.Query().Get(CompleteMultipartUploadQueryParam)
	o.AddLogFields(r, logging.Fields{"upload_id": uploadID})
	multiPart, err := o.MultipartsTracker.Get(o.Context(r), uploadID)
	if err != nil {
		o.Log(r).WithError(err).Error("could not read multipart record")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	objName := multiPart.PhysicalAddress
	o.AddLogFields(r, logging.Fields{"physical_address": objName})
	xmlMultipartComplete, err := ioutil.ReadAll(r.Body)
	if err != nil {
		o.Log(r).WithError(err).Error("could not read request body")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	var MultipartList block.MultipartUploadCompletion
	err = xml.Unmarshal(xmlMultipartComplete, &MultipartList)
	if err != nil {
		o.Log(r).WithError(err).Error("could not parse multipart XML on complete multipart")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	etag, size, err = o.BlockStore.CompleteMultiPartUpload(block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, uploadID, &MultipartList)
	if err != nil {
		o.Log(r).WithError(err).Error("could not complete multipart upload")
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	ch := trimQuotes(*etag)
	checksum := strings.Split(ch, "-")[0]
	err = o.finishUpload(r, o.Repository.StorageNamespace, checksum, objName, size)
	if err != nil {
		o.EncodeError(w, r, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.MultipartsTracker.Delete(o.Context(r), uploadID)
	if err != nil {
		o.Log(r).WithError(err).Warn("could not delete multipart record")
	}

	scheme := httputil.RequestScheme(r)
	location := fmt.Sprintf("%s://%s.%s/%s/%s", scheme, o.Repository, o.FQDN, o.Reference, o.Path)
	o.EncodeResponse(w, r, &serde.CompleteMultipartUploadResult{
		Location: location,
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		ETag:     *etag,
	}, http.StatusOK)
}

func (controller *PostObject) Handle(w http.ResponseWriter, r *http.Request, o *PathOperation) {
	// POST is only supported for CreateMultipartUpload/CompleteMultipartUpload
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
	_, mpuCreateParamExist := r.URL.Query()[CreateMultipartUploadQueryParam]
	if mpuCreateParamExist {
		controller.HandleCreateMultipartUpload(w, r, o)
		return
	}

	_, mpuCompleteParamExist := r.URL.Query()[CompleteMultipartUploadQueryParam]
	if mpuCompleteParamExist {
		controller.HandleCompleteMultipartUpload(w, r, o)
		return
	}
	// otherwise
	w.WriteHeader(http.StatusMethodNotAllowed)
}
