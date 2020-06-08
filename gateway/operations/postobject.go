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
	"github.com/treeverse/lakefs/permissions"
)

const (
	CreateMultipartUploadQueryParam   = "uploads"
	CompleteMultipartUploadQueryParam = "uploadId"
)

type PostObject struct{}

func (controller *PostObject) RequiredPermissions(request *http.Request, repoId, branchId, path string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repoId, path),
		},
	}, nil
}

func (controller *PostObject) HandleCreateMultipartUpload(o *PathOperation) {
	//var err error
	o.Incr("create_mpu")
	UUIDbytes := ([16]byte(uuid.New()))
	objName := hex.EncodeToString(UUIDbytes[:])
	uploadId, err := o.BlockStore.CreateMultiPartUpload(o.Repo.StorageNamespace, objName, o.Request)
	if err != nil {
		o.Log().WithError(err).Error("could not create multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.Index.CreateMultiPartUpload(o.Repo.Id, uploadId, o.Path, objName, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not write multipart upload to DB")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(&serde.InitiateMultipartUploadResult{
		Bucket:   o.Repo.Id,
		Key:      o.Path,
		UploadId: uploadId,
	}, http.StatusOK)

}

func trimQuotes(s string) string {
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func (controller *PostObject) HandleCompleteMultipartUpload(o *PathOperation) {
	var etag *string
	var size int64
	o.Incr("complete_mpu")
	uploadId := o.Request.URL.Query().Get(CompleteMultipartUploadQueryParam)
	multiPart, err := o.Index.ReadMultiPartUpload(o.Repo.Id, uploadId)
	if err != nil {
		o.Log().WithError(err).Error("could not read  multipart record")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	objName := multiPart.PhysicalAddress
	XMLmultiPartComplete, err := ioutil.ReadAll(o.Request.Body)
	var MultipartList block.MultipartUploadCompletion
	err = xml.Unmarshal([]byte(XMLmultiPartComplete), &MultipartList)
	if err != nil {
		o.Log().WithError(err).Error("could not parse multipart XML on complete multipart")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	etag, size, err = o.BlockStore.CompleteMultiPartUpload(o.Repo.StorageNamespace, objName, uploadId, &MultipartList)
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	ch := trimQuotes(*etag)
	checksum := strings.Split(ch, "-")[0]
	existingName, err := o.Index.CreateDedupEntryIfNone(o.Repo.Id, checksum, multiPart.PhysicalAddress)
	if err != nil {
		o.Log().WithError(err).Error("failed checking for duplicate content")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	if existingName != objName { // object already exist
		o.BlockStore.Remove(o.Repo.StorageNamespace, objName)
		objName = existingName
	}
	err = o.finishUpload(checksum, objName, size)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	err = o.Index.DeleteMultiPartUpload(o.Repo.Id, uploadId)
	if err != nil {
		o.Log().WithError(err).Warn("could not delete  multipart record")
	}

	// TODO: pass scheme instead of hard-coding http instead of https
	o.EncodeResponse(&serde.CompleteMultipartUploadResult{
		Location: fmt.Sprintf("http://%s.%s/%s/%s", o.Repo, o.FQDN, o.Ref, o.Path),
		Bucket:   o.Repo.Id,
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
