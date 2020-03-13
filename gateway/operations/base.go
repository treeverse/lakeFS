package operations

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/auth"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"

	log "github.com/sirupsen/logrus"
)

type Operation struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	Region         string
	FQDN           string

	Index            index.Index
	MultipartManager index.MultipartManager
	BlockStore       block.Adapter
	Auth             utils.GatewayService
}

func (o *Operation) RequestId() string {
	req, rid := httputil.RequestId(o.Request)
	o.Request = req
	return rid
}

func (o *Operation) Log() *log.Entry {
	return log.WithFields(log.Fields{
		"request_id": o.RequestId(),
		"region":     o.Region,
	})
}

func (o *Operation) EncodeXMLBytes(t []byte, statusCode int) {
	o.ResponseWriter.WriteHeader(statusCode)
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.Write(t)
	_, err := b.WriteTo(o.ResponseWriter)
	if err != nil {
		// TODO: log error?
		o.Log().WithError(err).Error("could not write response to HTTP client")
	}
}

func (o *Operation) EncodeResponse(entity interface{}, statusCode int) {
	//payload, err := xml.MarshalIndent(entity, "", "  ")
	// We don't indent the XML document because of Java.
	// See: https://github.com/spulec/moto/issues/1870
	payload, err := xml.Marshal(entity)
	if err != nil {
		o.Log().WithError(err).Error("could not marshal response to XML")
		o.ResponseWriter.WriteHeader(http.StatusInternalServerError)
		return
	}
	o.EncodeXMLBytes(payload, statusCode)
}

func (o *Operation) DecodeXMLBody(entity interface{}) error {
	body := o.Request.Body
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

func (o *Operation) EncodeError(err errors.APIError) {
	o.EncodeResponse(errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: "",
		Key:        "",
		Resource:   "",
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8), // just for compatibility, meaningless in our case
	}, err.HTTPStatusCode)
}

type AuthenticatedOperation struct {
	*Operation
	SubjectId   string
	SubjectType authmodel.APICredentials_Type
}

type RepoOperation struct {
	*AuthenticatedOperation
	Repo *model.Repo
}

func (o *RepoOperation) EncodeError(err errors.APIError) {
	o.EncodeResponse(errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repo.GetRepoId(),
		Key:        "",
		Resource:   o.Repo.GetRepoId(),
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
}

type RefOperation struct {
	*RepoOperation
	Ref string
}

type PathOperation struct {
	*RefOperation
	Path string
}

func (o *PathOperation) EncodeError(err errors.APIError) {
	o.EncodeResponse(errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repo.GetRepoId(),
		Key:        o.Path,
		Resource:   fmt.Sprintf("%s@%s", o.Ref, o.Repo.GetRepoId()),
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
}

type BaseOperationHandler interface {
	Action(req *http.Request) permissions.Action
}

type OperationHandler interface {
	BaseOperationHandler
	Handle(op *Operation)
}

type AuthenticatedOperationHandler interface {
	BaseOperationHandler
	Handle(op *AuthenticatedOperation)
}

type RepoOperationHandler interface {
	BaseOperationHandler
	Handle(op *RepoOperation)
}

type BranchOperationHandler interface {
	BaseOperationHandler
	Handle(op *RefOperation)
}
type PathOperationHandler interface {
	BaseOperationHandler
	Handle(op *PathOperation)
}
