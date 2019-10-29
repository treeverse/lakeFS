package gateway

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
	"versio-index/auth"
	authmodel "versio-index/auth/model"
	"versio-index/auth/sig"
	"versio-index/block"
	"versio-index/db"
	"versio-index/gateway/errors"
	"versio-index/gateway/operations"
	"versio-index/index"

	"golang.org/x/xerrors"

	"github.com/gorilla/mux"
)

const (
	RepoMatch   = "{repo:[a-z0-9]+}"
	KeyMatch    = "{key:.*}"
	BranchMatch = "{branch:[a-z0-9\\-]+}"
)

func getRepo(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["repo"]
}

func getKey(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["key"]
}

func getBranch(req *http.Request) string {
	vars := mux.Vars(req)
	return vars["branch"]
}

type Server struct {
	region      string
	meta        index.Index
	blockStore  block.Adapter
	authService auth.Service

	server *http.Server
	router mux.Router
}

func (s *Server) RegisterOperation(route *mux.Route, handler operations.AuthenticatedOperationHandler) {
	route.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := s.authenticateOperation(writer, request, handler.GetIntent(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	})
}

func (s *Server) RegisterRepoOperation(route *mux.Route, handler operations.RepoOperationHandler) {
	route.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := s.authenticateOperation(writer, request, handler.GetIntent(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(&operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repo:                   getRepo(request),
		})
	})
}

func (s *Server) RegisterPathOperation(route *mux.Route, handler operations.PathOperationHandler) {
	route.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := s.authenticateOperation(writer, request, handler.GetIntent(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(&operations.PathOperation{
			BranchOperation: &operations.BranchOperation{
				RepoOperation: &operations.RepoOperation{
					AuthenticatedOperation: authOp,
					Repo:                   getRepo(request),
				},
				Branch: getBranch(request),
			},
			Path: getKey(request),
		})
	})
}

func (s *Server) RegisterBranchOperation(route *mux.Route, handler operations.BranchOperationHandler) {
	route.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := s.authenticateOperation(writer, request, handler.GetIntent(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(&operations.BranchOperation{
			RepoOperation: &operations.RepoOperation{
				AuthenticatedOperation: authOp,
				Repo:                   getRepo(request),
			},
			Branch: getBranch(request),
		})
	})
}

func (s *Server) authenticateOperation(writer http.ResponseWriter, request *http.Request, intent authmodel.Permission_Intent, arn string) *operations.AuthenticatedOperation {
	o := &operations.Operation{
		Request:        request,
		ResponseWriter: writer,

		Region:     s.region,
		Index:      s.meta,
		BlockStore: s.blockStore,
		Auth:       s.authService,
	}
	// authenticate
	authContext, err := sig.ParseV4AuthContext(request)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	creds, err := s.authService.GetAPICredentials(authContext.AccessKeyId)
	if err != nil {
		if !xerrors.Is(err, db.ErrNotFound) {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		} else {
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		}
		return nil
	}

	err = sig.V4Verify(authContext, creds, request)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	// we are verified!
	op := &operations.AuthenticatedOperation{
		Operation:   o,
		ClientId:    creds.GetClientId(),
		SubjectId:   creds.GetEntityId(),
		SubjectType: creds.GetCredentialType(),
	}

	// interpolate arn string
	arn = auth.ResolveARN(arn, mux.Vars(request))

	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		ClientID:   op.ClientId,
		UserID:     op.SubjectId,
		Intent:     intent,
		SubjectARN: arn,
	})
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return nil
	}

	if authResp.Error != nil || !authResp.Allowed {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	// authentication and authorization complete!
	return op
}

type ResponseRecordingWriter struct {
	statusCode   int
	body         bytes.Buffer
	responseSize int64
	writer       http.ResponseWriter
}

func (w *ResponseRecordingWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *ResponseRecordingWriter) Write(data []byte) (int, error) {
	written, err := w.writer.Write(data)
	if err == nil {
		w.responseSize += int64(written)
		w.body.Write(data)
	}
	return written, err
}

func (w *ResponseRecordingWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.writer.WriteHeader(statusCode)
}

func NewServer(region string, meta index.Index, blockStore block.Adapter, authService auth.Service, listenAddr, bareDomain string) *Server {
	r := mux.NewRouter()
	s := &Server{
		meta:        meta,
		region:      region,
		blockStore:  blockStore,
		authService: authService,
		server: &http.Server{
			Handler: r,
			Addr:    listenAddr,
		},
	}

	// register middleware
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Do stuff here
			before := time.Now()
			fmt.Printf("%s %s\n", r.Method, r.RequestURI)
			// Call the next handler, which can be another middleware in the chain, or the final handler.
			writer := &ResponseRecordingWriter{writer: w}
			next.ServeHTTP(writer, r)
			fmt.Printf("-> took: %.2fms, statusCode: %d, sent: %d, bytes: %s\n", time.Since(before).Seconds()*1000.0, writer.statusCode, writer.responseSize, writer.body.String())
		})
	})

	// path based routing
	// non-bucket-specific endpoints
	serviceEndpoint := r.Host(bareDomain).Subrouter()
	// global level
	s.RegisterOperation(serviceEndpoint.PathPrefix("/").Methods(http.MethodGet), &operations.ListBuckets{})
	// repo-specific actions that relate to a key
	pathBasedRepo := serviceEndpoint.PathPrefix(fmt.Sprintf("/%s", RepoMatch)).Subrouter()
	pathBasedRepoWithKey := pathBasedRepo.PathPrefix(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	s.RegisterPathOperation(pathBasedRepoWithKey.Methods(http.MethodDelete), &operations.DeleteObject{})
	s.RegisterPathOperation(pathBasedRepoWithKey.Methods(http.MethodGet), &operations.GetObject{})
	s.RegisterPathOperation(pathBasedRepoWithKey.Methods(http.MethodHead), &operations.HeadObject{})
	s.RegisterPathOperation(pathBasedRepoWithKey.Methods(http.MethodPut), &operations.PutObject{})
	// bucket-specific actions that don't relate to a specific key
	s.RegisterRepoOperation(pathBasedRepo.Methods(http.MethodPut), &operations.CreateBucket{})
	s.RegisterRepoOperation(pathBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}"),
		&operations.ListObjects{})
	s.RegisterRepoOperation(pathBasedRepo.Methods(http.MethodDelete), &operations.DeleteBucket{})
	s.RegisterRepoOperation(pathBasedRepo.Methods(http.MethodHead), &operations.HeadBucket{})
	s.RegisterRepoOperation(pathBasedRepo.Methods(http.MethodPost), &operations.DeleteObjects{})

	// sub-domain based routing
	subDomainBasedRepo := r.Host(fmt.Sprintf("%s.%s", RepoMatch, bareDomain)).Subrouter()
	// repo-specific actions that relate to a key
	subDomainBasedRepoWithKey := subDomainBasedRepo.PathPrefix(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	s.RegisterPathOperation(subDomainBasedRepoWithKey.Methods(http.MethodDelete), &operations.DeleteObject{})
	s.RegisterPathOperation(subDomainBasedRepoWithKey.Methods(http.MethodGet), &operations.GetObject{})
	s.RegisterPathOperation(subDomainBasedRepoWithKey.Methods(http.MethodHead), &operations.HeadObject{})
	s.RegisterPathOperation(subDomainBasedRepoWithKey.Methods(http.MethodPut), &operations.PutObject{})
	// bucket-specific actions that don't relate to a specific key
	s.RegisterRepoOperation(subDomainBasedRepo.Path("/").Methods(http.MethodPut), &operations.CreateBucket{})
	s.RegisterRepoOperation(subDomainBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}"),
		&operations.ListObjects{})
	s.RegisterRepoOperation(subDomainBasedRepo.Path("/").Methods(http.MethodDelete), &operations.DeleteBucket{})
	s.RegisterRepoOperation(subDomainBasedRepo.Path("/").Methods(http.MethodHead), &operations.HeadBucket{})
	s.RegisterRepoOperation(subDomainBasedRepo.Path("/").Methods(http.MethodPost), &operations.DeleteObjects{})

	return s
}

func (s *Server) Listen() error {
	return s.server.ListenAndServe()
}

//CreateMultipartUpload
//
//CompleteMultipartUpload
//
//AbortMultipartUpload
//
//ListMultipartUploads
//
//UploadPart
