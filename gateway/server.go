package gateway

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"
	"treeverse-lake/auth"
	"treeverse-lake/auth/sig"
	"treeverse-lake/block"
	"treeverse-lake/db"
	"treeverse-lake/gateway/errors"
	ghttp "treeverse-lake/gateway/http"
	"treeverse-lake/gateway/operations"
	"treeverse-lake/index"

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

type ServerContext struct {
	region      string
	meta        index.Index
	blockStore  block.Adapter
	authService auth.Service
}

type Server struct {
	ctx        *ServerContext
	server     *http.Server
	bareDomain string
}

func NewServer(region string, meta index.Index, blockStore block.Adapter, authService auth.Service, listenAddr, bareDomain string) *Server {

	ctx := &ServerContext{
		meta:        meta,
		region:      region,
		blockStore:  blockStore,
		authService: authService,
	}

	// setup routes
	router := mux.NewRouter()
	attachDebug(router)
	attachRoutes(bareDomain, router, ctx)
	router.Use(ghttp.LoggingMiddleWare)

	// assemble server
	return &Server{
		ctx:        ctx,
		bareDomain: bareDomain,
		server: &http.Server{
			Handler: router,
			Addr:    listenAddr,
		},
	}
}

func (s *Server) Listen() error {
	return s.server.ListenAndServe()
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
		//Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(RepoOperationHandler(ctx, &operations.ListObjects{}))
	pathBasedRepo.Methods(http.MethodDelete).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteBucket{}))
	pathBasedRepo.Methods(http.MethodHead).HandlerFunc(RepoOperationHandler(ctx, &operations.HeadBucket{}))
	pathBasedRepo.Methods(http.MethodPost).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteObjects{}))
	// global level
	serviceEndpoint.PathPrefix("/").Methods(http.MethodGet).HandlerFunc(OperationHandler(ctx, &operations.ListBuckets{}))

	// sub-domain based routing
	subDomainBasedRepo := router.Host(strings.Join([]string{RepoMatch, bareDomain}, ".")).Subrouter()
	// repo-specific actions that relate to a key
	subDomainBasedRepoWithKey := subDomainBasedRepo.PathPrefix(fmt.Sprintf("/%s/%s", BranchMatch, KeyMatch)).Subrouter()
	subDomainBasedRepoWithKey.Methods(http.MethodDelete).HandlerFunc(PathOperationHandler(ctx, &operations.DeleteObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodGet).HandlerFunc(PathOperationHandler(ctx, &operations.GetObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodHead).HandlerFunc(PathOperationHandler(ctx, &operations.HeadObject{}))
	subDomainBasedRepoWithKey.Methods(http.MethodPut).HandlerFunc(PathOperationHandler(ctx, &operations.PutObject{}))
	// bucket-specific actions that don't relate to a specific key
	subDomainBasedRepo.
		Methods(http.MethodGet).
		//Queries("prefix", "{prefix}", "Prefix", "{prefix}", "Delimiter", "{delimiter}", "delimiter", "{delimiter}").
		HandlerFunc(RepoOperationHandler(ctx, &operations.ListObjects{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodDelete).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteBucket{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodHead).HandlerFunc(RepoOperationHandler(ctx, &operations.HeadBucket{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodPost).HandlerFunc(RepoOperationHandler(ctx, &operations.DeleteObjects{}))
	subDomainBasedRepo.Path("/").Methods(http.MethodPut).HandlerFunc(RepoOperationHandler(ctx, &operations.CreateBucket{}))
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
		o.Log().WithError(err).Warn("could not parse v4 auth context")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	creds, err := s.authService.GetAPICredentials(authContext.AccessKeyId)
	if err != nil {
		if !xerrors.Is(err, db.ErrNotFound) {
			o.Log().WithError(err).WithField("key", authContext.AccessKeyId).Warn("error getting access key")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		} else {
			o.Log().WithError(err).WithField("key", authContext.AccessKeyId).Warn("could not find access key")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		}
		return nil
	}

	err = sig.V4Verify(authContext, creds, request)
	if err != nil {
		o.Log().WithError(err).WithField("key", authContext.AccessKeyId).Warn("error verifying credentials for key")
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
		o.Log().WithError(authResp.Error).WithField("key", authContext.AccessKeyId).Warn("no permission")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	// authentication and authorization complete!
	return op
}
