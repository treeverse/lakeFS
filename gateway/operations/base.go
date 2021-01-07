package operations

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/gateway/multiparts"
	"github.com/treeverse/lakefs/gateway/simulator"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

const StorageClassHeader = "x-amz-storage-class"

const (
	OperationIDDeleteObject  = "delete_object"
	OperationIDDeleteObjects = "delete_objects"
	OperationIDGetObject     = "get_object"
	OperationIDHeadBucket    = "head_bucket"
	OperationIDHeadObject    = "head_object"
	OperationIDListBuckets   = "list_buckets"
	OperationIDListObjects   = "list_objects"
	OperationIDPostObject    = "post_object"
	OperationIDPutObject     = "put_object"

	OperationIDUnsupportedOperation = "unsupported"
	OperationIDOperationNotFound    = "not_found"
)

type ActionIncr func(string)

type Operation struct {
	OperationID       string
	Region            string
	FQDN              string
	Cataloger         catalog.Cataloger
	MultipartsTracker multiparts.Tracker
	BlockStore        block.Adapter
	Auth              simulator.GatewayAuthService
	Incr              ActionIncr
	DedupCleaner      *dedup.Cleaner
}

func StorageClassFromHeader(header http.Header) *string {
	storageClass := header.Get(StorageClassHeader)
	if storageClass == "" {
		return nil
	}
	return &storageClass
}

func (o *Operation) AddLogFields(req *http.Request, fields logging.Fields) {
	ctx := logging.AddFields(o.Context(req), fields)
	req = req.WithContext(ctx)
}

func (o *Operation) Context(req *http.Request) context.Context {
	return req.Context()
}

func (o *Operation) Log(req *http.Request) logging.Logger {
	return logging.FromContext(o.Context(req))
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
	content, err := ioutil.ReadAll(body)
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
func (o *Operation) SetHeaders(w http.ResponseWriter, headers map[string]string) {
	for k, v := range headers {
		o.SetHeader(w, k, v)
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
	return auth.HexStringGenerator(generatedHostIDLength)
}

type AuthenticatedOperation struct {
	*Operation
	Principal string
}

type RepoOperation struct {
	*AuthenticatedOperation
	Repository *catalog.Repository
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
	RequiredPermissions(req *http.Request) ([]permissions.Permission, error)
	Handle(w http.ResponseWriter, req *http.Request, op *Operation)
}

type AuthenticatedOperationHandler interface {
	RequiredPermissions(req *http.Request) ([]permissions.Permission, error)
	Handle(w http.ResponseWriter, req *http.Request, op *AuthenticatedOperation)
}

type RepoOperationHandler interface {
	RequiredPermissions(req *http.Request, repository string) ([]permissions.Permission, error)
	Handle(w http.ResponseWriter, req *http.Request, op *RepoOperation)
}

type BranchOperationHandler interface {
	RequiredPermissions(req *http.Request, repository, branch string) ([]permissions.Permission, error)
	Handle(w http.ResponseWriter, req *http.Request, op *RefOperation)
}
type PathOperationHandler interface {
	RequiredPermissions(req *http.Request, repository, branch, path string) ([]permissions.Permission, error)
	Handle(w http.ResponseWriter, req *http.Request, op *PathOperation)
}
