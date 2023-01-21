package operations

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/upload"
)

const StorageClassHeader = "x-amz-storage-class"

type OperationID string

const (
	OperationIDDeleteObject  OperationID = "delete_object"
	OperationIDDeleteObjects OperationID = "delete_objects"
	OperationIDGetObject     OperationID = "get_object"
	OperationIDHeadBucket    OperationID = "head_bucket"
	OperationIDHeadObject    OperationID = "head_object"
	OperationIDListBuckets   OperationID = "list_buckets"
	OperationIDListObjects   OperationID = "list_objects"
	OperationIDPostObject    OperationID = "post_object"
	OperationIDPutObject     OperationID = "put_object"
	OperationIDPutBucket     OperationID = "put_bucket"

	OperationIDUnsupportedOperation OperationID = "unsupported"
	OperationIDOperationNotFound    OperationID = "not_found"
)

type ActionIncr func(action, userID, repository, ref string)

type Operation struct {
	OperationID      OperationID
	Region           string
	FQDN             string
	Catalog          catalog.Interface
	MultipartTracker multipart.Tracker
	BlockStore       block.Adapter
	Auth             auth.GatewayService
	Incr             ActionIncr
	MatchedHost      bool
	PathProvider     upload.PathProvider
}

func StorageClassFromHeader(header http.Header) *string {
	storageClass := header.Get(StorageClassHeader)
	if storageClass == "" {
		return nil
	}
	return &storageClass
}

func (o *Operation) Log(req *http.Request) logging.Logger {
	return logging.FromContext(req.Context())
}

func EncodeXMLBytes(w http.ResponseWriter, t []byte, statusCode int) error {
	w.WriteHeader(statusCode)
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.Write(t)
	_, err := b.WriteTo(w)
	return err
}

func (o *Operation) EncodeXMLBytes(w http.ResponseWriter, req *http.Request, t []byte, statusCode int) {
	err := EncodeXMLBytes(w, t, statusCode)
	if err != nil {
		o.Log(req).WithError(err).Error("failed to encode XML to response")
	}
}

func EncodeResponse(w http.ResponseWriter, entity interface{}, statusCode int) error {
	// We don't indent the XML document because of Java.
	// See: https://github.com/spulec/moto/issues/1870
	payload, err := xml.Marshal(entity)
	if err != nil {
		return err
	}
	return EncodeXMLBytes(w, payload, statusCode)
}

func (o *Operation) EncodeResponse(w http.ResponseWriter, req *http.Request, entity interface{}, statusCode int) {
	err := EncodeResponse(w, entity, statusCode)
	if err != nil {
		o.Log(req).WithError(err).Error("encoding response failed")
	}
}

func DecodeXMLBody(reader io.Reader, entity interface{}) error {
	body := reader
	content, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	err = xml.Unmarshal(content, entity)
	if err != nil {
		return err
	}
	return nil
}

// SetHeader sets a header on the response while preserving its case
func (o *Operation) SetHeader(w http.ResponseWriter, key, value string) {
	w.Header()[key] = []string{value}
}

// DeleteHeader deletes a header from the response
func (o *Operation) DeleteHeader(w http.ResponseWriter, key string) {
	w.Header().Del(key)
}

// SetHeaders sets a map of headers on the response while preserving the header's case
func (o *Operation) SetHeaders(w http.ResponseWriter, headers http.Header) {
	h := w.Header()
	for k, v := range headers {
		for _, val := range v {
			h.Add(k, val)
		}
	}
}

func (o *Operation) EncodeError(w http.ResponseWriter, req *http.Request, e errors.APIError) *http.Request {
	req, rid := httputil.RequestID(req)
	err := EncodeResponse(w, errors.APIErrorResponse{
		Code:       e.Code,
		Message:    e.Description,
		BucketName: "",
		Key:        "",
		Resource:   "",
		Region:     o.Region,
		RequestID:  rid,
		HostID:     generateHostID(), // just for compatibility, meaningless in our case
	}, e.HTTPStatusCode)
	if err != nil {
		o.Log(req).WithError(err).Error("encoding response failed")
	}
	return req
}

func generateHostID() string {
	const generatedHostIDLength = 8
	return keys.HexStringGenerator(generatedHostIDLength)
}

type AuthorizedOperation struct {
	*Operation
	Principal string
}

type RepoOperation struct {
	*AuthorizedOperation
	Repository  *catalog.Repository
	MatchedHost bool
}

func (o *RepoOperation) EncodeError(w http.ResponseWriter, req *http.Request, err errors.APIError) *http.Request {
	req, rid := httputil.RequestID(req)
	writeErr := EncodeResponse(w, errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repository.Name,
		Key:        "",
		Resource:   o.Repository.Name,
		Region:     o.Region,
		RequestID:  rid,
		HostID:     generateHostID(),
	}, err.HTTPStatusCode)
	if writeErr != nil {
		o.Log(req).WithError(writeErr).Error("encoding response failed")
	}
	return req
}

type RefOperation struct {
	*RepoOperation
	Reference string
}

type PathOperation struct {
	*RefOperation
	Path string
}

func (o *PathOperation) EncodeError(w http.ResponseWriter, req *http.Request, err errors.APIError) *http.Request {
	req, rid := httputil.RequestID(req)
	writeErr := EncodeResponse(w, errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repository.Name,
		Key:        o.Path,
		Resource:   fmt.Sprintf("%s@%s", o.Reference, o.Repository.Name),
		Region:     o.Region,
		RequestID:  rid,
		HostID:     generateHostID(),
	}, err.HTTPStatusCode)
	if writeErr != nil {
		o.Log(req).WithError(writeErr).Error("encoding response failed")
	}
	return req
}

type OperationHandler interface {
	RequiredPermissions(req *http.Request) (permissions.Node, error)
	Handle(w http.ResponseWriter, req *http.Request, op *Operation)
}

type AuthenticatedOperationHandler interface {
	RequiredPermissions(req *http.Request) (permissions.Node, error)
	Handle(w http.ResponseWriter, req *http.Request, op *AuthorizedOperation)
}

type RepoOperationHandler interface {
	RequiredPermissions(req *http.Request, repository string) (permissions.Node, error)
	Handle(w http.ResponseWriter, req *http.Request, op *RepoOperation)
}

type BranchOperationHandler interface {
	RequiredPermissions(req *http.Request, repository, branch string) (permissions.Node, error)
	Handle(w http.ResponseWriter, req *http.Request, op *RefOperation)
}

type PathOperationHandler interface {
	RequiredPermissions(req *http.Request, repository, branch, path string) (permissions.Node, error)
	Handle(w http.ResponseWriter, req *http.Request, op *PathOperation)
}
