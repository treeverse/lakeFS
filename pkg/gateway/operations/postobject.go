package operations

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	gatewayErrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	CreateMultipartUploadQueryParam   = "uploads"
	CompleteMultipartUploadQueryParam = "uploadId"
)

type PostObject struct{}

func (controller *PostObject) RequiredPermissions(_ *http.Request, repoID, _, path string) (permissions.Node, error) {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.WriteObjectAction,
			Resource: permissions.ObjectArn(repoID, path),
		},
	}, nil
}

func (controller *PostObject) HandleCreateMultipartUpload(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("create_mpu", o.Principal, o.Repository.Name, o.Reference)
	branchExists, err := o.Catalog.BranchExists(req.Context(), o.Repository.Name, o.Reference)
	if err != nil {
		o.Log(req).WithError(err).Error("could not check if branch exists")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	if !branchExists {
		o.Log(req).Debug("branch not found")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrNoSuchBucket))
		return
	}
	address := o.PathProvider.NewPath()
	storageClass := StorageClassFromHeader(req.Header)
	opts := block.CreateMultiPartUploadOpts{StorageClass: storageClass}
	resp, err := o.BlockStore.CreateMultiPartUpload(req.Context(), block.ObjectPointer{
		StorageNamespace: o.Repository.StorageNamespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       address,
	}, req, opts)
	if err != nil {
		o.Log(req).WithError(err).Error("could not create multipart upload")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	mpu := multipart.Upload{
		UploadID:        resp.UploadID,
		Path:            o.Path,
		CreationDate:    time.Now(),
		PhysicalAddress: address,
		Metadata:        map[string]string(amzMetaAsMetadata(req)),
		ContentType:     req.Header.Get("Content-Type"),
	}
	err = o.MultipartTracker.Create(req.Context(), mpu)
	if err != nil {
		o.Log(req).WithError(err).Error("could not write multipart upload to DB")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	o.SetHeaders(w, resp.ServerSideHeader)
	o.EncodeResponse(w, req, &serde.InitiateMultipartUploadResult{
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		UploadID: resp.UploadID,
	}, http.StatusOK)
}

func (controller *PostObject) HandleCompleteMultipartUpload(w http.ResponseWriter, req *http.Request, o *PathOperation) {
	o.Incr("complete_mpu", o.Principal, o.Repository.Name, o.Reference)
	uploadID := req.URL.Query().Get(CompleteMultipartUploadQueryParam)
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{logging.UploadIDFieldKey: uploadID}))
	multiPart, err := o.MultipartTracker.Get(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read multipart record")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	objName := multiPart.PhysicalAddress
	req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{logging.PhysicalAddressFieldKey: objName}))
	xmlMultipartComplete, err := io.ReadAll(req.Body)
	if err != nil {
		o.Log(req).WithError(err).Error("could not read request body")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	var multipartList block.MultipartUploadCompletion
	err = xml.Unmarshal(xmlMultipartComplete, &multipartList)
	if err != nil {
		o.Log(req).WithError(err).Error("could not parse multipart XML on complete multipart")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	normalizeMultipartUploadCompletion(&multipartList)
	resp, err := o.BlockStore.CompleteMultiPartUpload(req.Context(),
		block.ObjectPointer{
			StorageNamespace: o.Repository.StorageNamespace,
			IdentifierType:   block.IdentifierTypeRelative,
			Identifier:       objName,
		},
		uploadID,
		&multipartList)
	if err != nil {
		o.Log(req).WithError(err).Error("could not complete multipart upload")
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	checksum := strings.Split(resp.ETag, "-")[0]
	err = o.finishUpload(req, checksum, objName, resp.ContentLength, true, multiPart.Metadata, multiPart.ContentType)
	if errors.Is(err, graveler.ErrWriteToProtectedBranch) {
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrWriteToProtectedBranch))
		return
	}
	if err != nil {
		_ = o.EncodeError(w, req, gatewayErrors.Codes.ToAPIErr(gatewayErrors.ErrInternalError))
		return
	}
	err = o.MultipartTracker.Delete(req.Context(), uploadID)
	if err != nil {
		o.Log(req).WithError(err).Warn("could not delete multipart record")
	}

	scheme := httputil.RequestScheme(req)
	var location string
	if o.MatchedHost {
		location = fmt.Sprintf("%s://%s/%s/%s", scheme, req.Host, o.Reference, o.Path)
	} else {
		location = fmt.Sprintf("%s://%s/%s/%s/%s", scheme, req.Host, o.Repository.Name, o.Reference, o.Path)
	}
	o.SetHeaders(w, resp.ServerSideHeader)
	o.EncodeResponse(w, req, &serde.CompleteMultipartUploadResult{
		Location: location,
		Bucket:   o.Repository.Name,
		Key:      path.WithRef(o.Path, o.Reference),
		ETag:     httputil.ETag(resp.ETag),
	}, http.StatusOK)
}

// normalizeMultipartUploadCompletion normalization incoming multipart upload completion list.
// we make sure that each part's ETag will be without the wrapping quotes
func normalizeMultipartUploadCompletion(list *block.MultipartUploadCompletion) {
	for i := range list.Part {
		list.Part[i].ETag = strings.Trim(list.Part[i].ETag, `"`)
	}
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
