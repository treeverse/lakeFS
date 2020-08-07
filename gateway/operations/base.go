package operations

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

const StorageClassHeader = "x-amz-storage-class"

type ActionIncr func(string)

type Operation struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	Region         string
	FQDN           string
	Cataloger      catalog.Cataloger
	BlockStore     block.Adapter
	Auth           simulator.GatewayAuthService
	Incr           ActionIncr
	DedupCleaner   *dedup.Cleaner
}

func (o *Operation) RequestID() string {
	req, rid := httputil.RequestID(o.Request)
	o.Request = req
	return rid
}

func StorageClassFromHeader(header http.Header) *string {
	storageClass := header.Get(StorageClassHeader)
	if storageClass == "" {
		return nil
	}
	return &storageClass
}

func (o *Operation) AddLogFields(fields logging.Fields) {
	ctx := logging.AddFields(o.Context(), fields)
	o.Request = o.Request.WithContext(ctx)
}

func (o *Operation) Context() context.Context {
	return o.Request.Context()
}

func (o *Operation) Log() logging.Logger {
	return logging.FromContext(o.Context())
}

func EncodeXMLBytes(w http.ResponseWriter, t []byte, statusCode int) error {
	w.WriteHeader(statusCode)
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.Write(t)
	_, err := b.WriteTo(w)
	return err
}

func (o *Operation) EncodeXMLBytes(t []byte, statusCode int) {
	err := EncodeXMLBytes(o.ResponseWriter, t, statusCode)
	if err != nil {
		o.Log().WithError(err).Error("failed to encode XML to response")
	}
}

func EncodeResponse(w http.ResponseWriter, entity interface{}, statusCode int) error {
	//payload, err := xml.MarshalIndent(entity, "", "  ")
	// We don't indent the XML document because of Java.
	// See: https://github.com/spulec/moto/issues/1870
	payload, err := xml.Marshal(entity)
	if err != nil {
		return err
	}
	return EncodeXMLBytes(w, payload, statusCode)
}

func (o *Operation) EncodeResponse(entity interface{}, statusCode int) {
	err := EncodeResponse(o.ResponseWriter, entity, statusCode)
	if err != nil {
		o.Log().WithError(err).Error("encoding response failed")
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
func (o *Operation) SetHeader(key, value string) {
	o.ResponseWriter.Header()[key] = []string{value}
}

// SetHeaders sets a map of headers on the response while preserving the header's case
func (o *Operation) SetHeaders(headers map[string]string) {
	for k, v := range headers {
		o.SetHeader(k, v)
	}
}

func (o *Operation) EncodeError(e errors.APIError) {
	err := EncodeResponse(o.ResponseWriter, errors.APIErrorResponse{
		Code:       e.Code,
		Message:    e.Description,
		BucketName: "",
		Key:        "",
		Resource:   "",
		Region:     o.Region,
		RequestID:  o.RequestID(),
		HostID:     auth.HexStringGenerator(8), // just for compatibility, meaningless in our case
	}, e.HTTPStatusCode)
	if err != nil {
		o.Log().WithError(err).Error("encoding response failed")
	}
}

type AuthenticatedOperation struct {
	*Operation
	Principal string
}

type RepoOperation struct {
	*AuthenticatedOperation
	Repository *catalog.Repository
}

func (o *RepoOperation) EncodeError(err errors.APIError) {
	writeErr := EncodeResponse(o.ResponseWriter, errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repository.Name,
		Key:        "",
		Resource:   o.Repository.Name,
		Region:     o.Region,
		RequestID:  o.RequestID(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
	if writeErr != nil {
		o.Log().WithError(writeErr).Error("encoding response failed")
	}
}

type RefOperation struct {
	*RepoOperation
	Reference string
}

type PathOperation struct {
	*RefOperation
	Path string
}

func (o *PathOperation) EncodeError(err errors.APIError) {
	writeErr := EncodeResponse(o.ResponseWriter, errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repository.Name,
		Key:        o.Path,
		Resource:   fmt.Sprintf("%s@%s", o.Reference, o.Repository.Name),
		Region:     o.Region,
		RequestID:  o.RequestID(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
	if writeErr != nil {
		o.Log().WithError(writeErr).Error("encoding response failed")
	}
}

type OperationHandler interface {
	RequiredPermissions(request *http.Request) ([]permissions.Permission, error)
	Handle(op *Operation)
}

type AuthenticatedOperationHandler interface {
	RequiredPermissions(request *http.Request) ([]permissions.Permission, error)
	Handle(op *AuthenticatedOperation)
}

type RepoOperationHandler interface {
	RequiredPermissions(request *http.Request, repository string) ([]permissions.Permission, error)
	Handle(op *RepoOperation)
}

type BranchOperationHandler interface {
	RequiredPermissions(request *http.Request, repository, branch string) ([]permissions.Permission, error)
	Handle(op *RefOperation)
}
type PathOperationHandler interface {
	RequiredPermissions(request *http.Request, repository, branch, path string) ([]permissions.Permission, error)
	Handle(op *PathOperation)
}
