package operations

import (
	"fmt"
	"net/http"
	"time"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/gateway/serde"
	"treeverse-lake/ident"
)

const (
	CreateMultipartUploadQueryParam   = "uploads"
	CompleteMultipartUploadQueryParam = "uploadId"
)

type PostObject struct{}

func (controller *PostObject) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *PostObject) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *PostObject) HandleCreateMultipartUpload(o *PathOperation) {
	uploadId, err := o.MultipartManager.Create(o.Repo, o.Path, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not create multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(&serde.InitiateMultipartUploadResult{
		Bucket:   o.Repo,
		Key:      o.Path,
		UploadId: uploadId,
	}, http.StatusOK)
}

func (controller *PostObject) HandleCompleteMultipartUpload(o *PathOperation) {
	uploadId := o.Request.URL.Query().Get(CompleteMultipartUploadQueryParam)

	req := &serde.CompleteMultipartUpload{}
	err := o.DecodeXMLBody(req)
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload - cannot read XML body")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	obj, err := o.MultipartManager.Complete(o.Repo, o.Branch, o.Path, uploadId, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload - cannot write metadata")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	o.EncodeResponse(&serde.CompleteMultipartUploadResult{
		Location: fmt.Sprintf("http://%s.s3.local:8000/%s/%s", o.Repo, o.Branch, o.Path),
		Bucket:   o.Repo,
		Key:      o.Path,
		ETag:     fmt.Sprintf("\"%s\"", ident.Hash(obj)),
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
