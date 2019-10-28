package operations

import (
	"context"
	"encoding/xml"
	"net/http"
	"versio-index/auth"
	authmodel "versio-index/auth/model"
	"versio-index/block"
	"versio-index/gateway/errors"
	"versio-index/index"
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
	var reqId string
	ctx := o.Request.Context()
	resp := ctx.Value("request_id")
	if resp == nil {
		// assign a request ID for this request
		reqId = auth.HexStringGenerator(8)
		o.Request = o.Request.WithContext(context.WithValue(ctx, "request_id", reqId))
	}
	return reqId
}

func (o *Operation) EncodeResponse(entity interface{}, statusCode int) {
	encoder := xml.NewEncoder(o.ResponseWriter)
	err := encoder.Encode(entity)
	if err != nil {
		o.ResponseWriter.WriteHeader(http.StatusInternalServerError)
		return
	} else if statusCode != http.StatusOK {
		o.ResponseWriter.WriteHeader(statusCode)
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
		HostID:     auth.HexStringGenerator(8),
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
		Resource:   o.Repo,
		Region:     o.Region,
		RequestID:  o.RequestId(),
		HostID:     auth.HexStringGenerator(8),
	}, err.HTTPStatusCode)
}

type BaseOperationHandler interface {
	GetArn() string
	GetIntent() authmodel.Permission_Intent
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
