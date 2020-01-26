package operations

import (
	"fmt"
	"net/http"
	"time"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/gateway/serde"
	"treeverse-lake/index/model"
)

const (
	CreateMultipartUploadQueryParam   = "uploads"
	CompleteMultipartUploadQueryParam = "uploadId"
)

type PostObject struct{}

func (controller *PostObject) GetArn() string {
	return "arn:treeverse:repos:::{repo}"
}

func (controller *PostObject) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *PostObject) HandleCreateMultipartUpload(o *PathOperation) {
	uploadId, err := o.MultipartManager.Create(o.Repo.GetRepoId(), o.Path, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not create multipart upload")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.EncodeResponse(&serde.InitiateMultipartUploadResult{
		Bucket:   o.Repo.GetRepoId(),
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
	uploadId := o.Request.URL.Query().Get(CompleteMultipartUploadQueryParam)

	req := &serde.CompleteMultipartUpload{}
	err := o.DecodeXMLBody(req)
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload - cannot read XML body")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	parts := make([]*model.MultipartUploadPartRequest, len(req.Part))
	for i, part := range req.Part {
		parts[i] = &model.MultipartUploadPartRequest{
			PartNumber: int32(part.PartNumber),
			Etag:       trimQuotes(part.ETag), // XML structure is weird and comes with quotes around the ETag
		}
	}

	obj, err := o.MultipartManager.Complete(o.Repo.GetRepoId(), o.Branch, o.Path, uploadId, parts, time.Now())
	if err != nil {
		o.Log().WithError(err).Error("could not complete multipart upload - cannot write metadata")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrBadRequest))
		return
	}

	// TODO: pass scheme instead of hard-coding http instead of https
	o.EncodeResponse(&serde.CompleteMultipartUploadResult{
		Location: fmt.Sprintf("http://%s.%s/%s/%s", o.Repo, o.FQDN, o.Branch, o.Path),
		Bucket:   o.Repo.GetRepoId(),
		Key:      o.Path,
		ETag:     serde.ETag(obj.GetChecksum()),
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
