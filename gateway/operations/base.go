package operations

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"versio-index/auth"
	authmodel "versio-index/auth/model"
	"versio-index/block"
	"versio-index/gateway/errors"
	ghttp "versio-index/gateway/http"
	"versio-index/index"

	log "github.com/sirupsen/logrus"
)

type Operation struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	Region         string

	Index      index.Index
	BlockStore block.Adapter
	Auth       auth.Service
}

func (o *Operation) RequestId() string {
	req, rid := ghttp.RequestId(o.Request)
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
	payload, err := xml.MarshalIndent(entity, "", "  ")
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
	ClientId    string
	SubjectId   string
	SubjectType authmodel.APICredentials_Type
}

type RepoOperation struct {
	*AuthenticatedOperation
	Repo string
}

func (o *RepoOperation) EncodeError(err errors.APIError) {
	o.EncodeResponse(errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repo,
		Key:        "",
		Resource:   o.Repo,
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
}

type BranchOperation struct {
	*RepoOperation
	Branch string
}

type PathOperation struct {
	*BranchOperation
	Path string
}

func (o *PathOperation) EncodeError(err errors.APIError) {
	o.EncodeResponse(errors.APIErrorResponse{
		Code:       err.Code,
		Message:    err.Description,
		BucketName: o.Repo,
		Key:        o.Path,
		Resource:   fmt.Sprintf("%s@%s", o.Branch, o.Repo),
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
}

type BaseOperationHandler interface {
	GetArn() string
	GetPermission() string
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
	Handle(op *BranchOperation)
}
type PathOperationHandler interface {
	BaseOperationHandler
	Handle(op *PathOperation)
}
