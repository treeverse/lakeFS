package gateway

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"
	"versio-index/auth"
	"versio-index/auth/sig"
	"versio-index/block"
	"versio-index/db"
	"versio-index/gateway/errors"
	ghttp "versio-index/gateway/http"
	"versio-index/gateway/operations"
	"versio-index/index"

	"golang.org/x/xerrors"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
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

type ServerContext struct {
	region      string
	meta        index.Index
	blockStore  block.Adapter
	authService auth.Service
}

type Server struct {
	ctx        *ServerContext
	server     *http.Server
	router     mux.Router
	bareDomain string
}

func NewServer(region string, meta index.Index, blockStore block.Adapter, authService auth.Service, listenAddr, bareDomain string) *Server {

	ctx := &ServerContext{
		meta:        meta,
		region:      region,
		blockStore:  blockStore,
		authService: authService,
	}

	router := mux.NewRouter()
	attachDebug(router)
	attachRoutes(bareDomain, router, ctx)

	s := &Server{
		ctx:        ctx,
		bareDomain: bareDomain,
		server: &http.Server{
			Handler: router,
			Addr:    listenAddr,
		},
	}

	// register middleware
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Do stuff here
			before := time.Now()
			// Call the next handler, which can be another middleware in the chain, or the final handler.
			writer := &ghttp.ResponseRecordingWriter{Writer: w}
			r, reqId := ghttp.RequestId(r)
			next.ServeHTTP(writer, r)
			log.WithFields(log.Fields{
				"request_id":  reqId,
				"path":        r.RequestURI,
				"method":      r.Method,
				"took":        time.Since(before),
				"status_code": writer.StatusCode,
				"sent_bytes":  writer.ResponseSize,
			}).Debug("S3 gateway called")
		})
	})
	return s
}

func attachDebug(router *mux.Router) {

	r := router.Host("localhost:8000").Subrouter()
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

func attachRoutes(bareDomain string, router *mux.Router, ctx *ServerContext) {
	// path based routing
	// non-bucket-specific endpoints
	serviceEndpoint := router.Host(bareDomain).Subrouter()
	// global level
	serviceEndpoint.PathPrefix("/").Methods(http.MethodGet).HandlerFunc(OperationHandler(ctx, &operations.ListBuckets{}))
	// repo-specific actions that relate to a key
	pathBasedRepo := serviceEndpoint.PathPrefix(fmt.Sprintf("/%s", RepoMatch)).Subrouter()
	pathBasedRepoWithKey := pathBasedRepo.PathPrefix(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	pathBasedRepoWithKey.Methods(http.MethodDelete).HandlerFunc(PathOperationHandler(ctx, &operations.DeleteObject{}))
	pathBasedRepoWithKey.Methods(http.MethodGet).HandlerFunc(PathOperationHandler(ctx, &operations.GetObject{}))
	pathBasedRepoWithKey.Methods(http.MethodHead).HandlerFunc(PathOperationHandler(ctx, &operations.HeadObject{}))
	pathBasedRepoWithKey.Methods(http.MethodPut).HandlerFunc(PathOperationHandler(ctx, &operations.PutObject{}))
	// bucket-specific actions that don't relate to a specific key
	pathBasedRepo.Methods(http.MethodPut).HandlerFunc(RepoOperationHandler(ctx, &operations.CreateBucket{}))
	pathBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(RepoOperationHandler(ctx, &operations.ListObjects{}))

	pathBasedRepo.Methods(http.MethodDelete).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteBucket{}))
	pathBasedRepo.Methods(http.MethodHead).HandlerFunc(RepoOperationHandler(ctx, &operations.HeadBucket{}))
	pathBasedRepo.Methods(http.MethodPost).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteObjects{}))

	// sub-domain based routing
	subDomainBasedRepo := router.Host(strings.Join([]string{RepoMatch, bareDomain}, ".")).Subrouter()
	// repo-specific actions that relate to a key
	subDomainBasedRepoWithKey := subDomainBasedRepo.PathPrefix(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	subDomainBasedRepoWithKey.Methods(http.MethodDelete).HandlerFunc(PathOperationHandler(ctx, &operations.DeleteObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodGet).HandlerFunc(PathOperationHandler(ctx, &operations.GetObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodHead).HandlerFunc(PathOperationHandler(ctx, &operations.HeadObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodPut).HandlerFunc(PathOperationHandler(ctx, &operations.PutObject{}))
	// bucket-specific actions that don't relate to a specific key
	subDomainBasedRepo.Path("/").Methods(http.MethodPut).HandlerFunc(RepoOperationHandler(ctx, &operations.CreateBucket{}))
	subDomainBasedRepo.
		Methods(http.MethodGet).
		Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(RepoOperationHandler(ctx, &operations.ListObjects{}))

	subDomainBasedRepo.Path("/").Methods(http.MethodDelete).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteBucket{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodHead).HandlerFunc(RepoOperationHandler(ctx, &operations.HeadBucket{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodPost).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteObjects{}))
}

func (s *Server) Listen() error {
	return s.server.ListenAndServe()
}

func OperationHandler(ctx *ServerContext, handler operations.AuthenticatedOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := authenticateOperation(ctx, writer, request, handler.GetPermission(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	}
}

func RepoOperationHandler(ctx *ServerContext, handler operations.RepoOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := authenticateOperation(ctx, writer, request, handler.GetPermission(), handler.GetArn())
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(&operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repo:                   getRepo(request),
		})
	}
}

func PathOperationHandler(ctx *ServerContext, handler operations.PathOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := authenticateOperation(ctx, writer, request, handler.GetPermission(), handler.GetArn())
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
	}
}

func BranchOperationHandler(ctx *ServerContext, handler operations.BranchOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		authOp := authenticateOperation(ctx, writer, request, handler.GetPermission(), handler.GetArn())
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
	}
}

func authenticateOperation(s *ServerContext, writer http.ResponseWriter, request *http.Request, permission, arn string) *operations.AuthenticatedOperation {
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
		Permission: permission,
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

//CreateMultipartUpload
//
//CompleteMultipartUpload
//
//AbortMultipartUpload
//
//ListMultipartUploads
//
//UploadPart
