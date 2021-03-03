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

func (controller *PostObject) HandleCreateMultipartUpload(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("create_mpu")
	uuidBytes := [16]byte(uuid.New())
	objName := hex.EncodeToString(uuidBytes[:])
	storageClass := StorageClassFromHeader(req.Header)
	opts := block.CreateMultiPartUploadOpts{StorageClass: storageClass}
	uploadID, err := o.BlockStore.CreateMultiPartUpload(req.Context(), block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, req, opts)
	if err != nil {
		o.Log(req).WithError(err).Error("could not create multipart upload")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.MultipartsTracker.Create(req.Context(), uploadID, o.Path, objName, time.Now())
	if err != nil {
		o.Log(req).WithError(err).Error("could not write multipart upload to DB")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(w, req, &serde.InitiateMultipartUploadResult{
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		UploadID: uploadID,
	}, http.StatusOK)
}

func trimQuotes(s string) string {
	return strings.Trim(s, "\"")
}

func (controller *PostObject) HandleCompleteMultipartUpload(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	var etag *string
	var size int64
	o.Incr("complete_mpu")
	uploadID := req.URL.Query().Get(CompleteMultipartUploadQueryParam)
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{"upload_id": uploadID}))
	multiPart, err := o.MultipartsTracker.Get(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read multipart record")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	objName := multiPart.PhysicalAddress
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{"physical_address": objName}))
	xmlMultipartComplete, err := ioutil.ReadAll(req.Body)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read request body")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	var MultipartList block.MultipartUploadCompletion
	err = xml.Unmarshal(xmlMultipartComplete, &MultipartList)
	if err != nil {
		o.Log(req).WithError(err).Error("could not parse multipart XML on complete multipart")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	etag, size, err = o.BlockStore.CompleteMultiPartUpload(req.Context(), block.ObjectPointer{StorageNamespace: o.Repository.StorageNamespace, Identifier: objName}, uploadID, &MultipartList)
	if err != nil {
		o.Log(req).WithError(err).Error("could not complete multipart upload")
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	ch := trimQuotes(*etag)
	checksum := strings.Split(ch, "-")[0]
	err = o.finishUpload(req, checksum, objName, size)
	if err != nil {
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.MultipartsTracker.Delete(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Warn("could not delete multipart record")
	}

	scheme := httputil.RequestScheme(req)
	location := fmt.Sprintf("%s://%s.%s/%s/%s", scheme, o.Repository, o.FQDN, o.Reference, o.Path)
	o.EncodeResponse(w, req, &serde.CompleteMultipartUploadResult{
		Location: location,
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		ETag:     *etag,
	}, http.StatusOK)
}

func (controller *PostObject) Handle(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	// POST is only supported for CreateMultipartUpload/CompleteMultipartUpload
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
	_, mpuCreateParamExist := req.URL.Query()[CreateMultipartUploadQueryParam]
	if mpuCreateParamExist {
		controller.HandleCreateMultipartUpload(w, req, o)
		return
	}

	_, mpuCompleteParamExist := req.URL.Query()[CompleteMultipartUploadQueryParam]
	if mpuCompleteParamExist {
		controller.HandleCompleteMultipartUpload(w, req, o)
		return
	}
	// otherwise
	w.WriteHeader(http.StatusMethodNotAllowed)
}
