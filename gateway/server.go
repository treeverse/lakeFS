package gateway

import (
	"context"
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
)

type ServerContext struct {
	region      string
	bareDomain  string
	meta        index.Index
	blockStore  block.Adapter
	authService simulator.GatewayAuthService
	stats       stats.Collector
}

func (c *ServerContext) WithContext(ctx context.Context) *ServerContext {
	return &ServerContext{
		region:      c.region,
		bareDomain:  c.bareDomain,
		meta:        c.meta.WithContext(ctx),
		blockStore:  c.blockStore.WithContext(ctx),
		authService: c.authService,
		stats:       c.stats,
	}
}

type Server struct {
	ctx        *ServerContext
	Server     *http.Server
	bareDomain string
}

func NewServer(
	region string,
	meta index.Index,
	blockStore block.Adapter,
	authService simulator.GatewayAuthService,
	listenAddr, bareDomain string,
	stats stats.Collector,
) *Server {
	ctx := &ServerContext{
		meta:        meta,
		region:      region,
		bareDomain:  bareDomain,
		blockStore:  blockStore,
		authService: authService,
		stats:       stats,
	}

	// setup routes
	var handler http.Handler
	handler = &Handler{
		BareDomain:         bareDomain,
		ctx:                ctx,
		NotFoundHandler:    http.HandlerFunc(notFound),
		ServerErrorHandler: nil,
	}
	handler = simulator.RegisterRecorder(httputil.LoggingMiddleware(
		"X-Amz-Request-Id", logging.Fields{"service_name": "s3_gateway"}, handler,
	), authService, region, bareDomain, listenAddr)

	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomain,
		"s3_region":      region,
	}).Info("initialized S3 Gateway server")

	// assemble Server
	return &Server{
		ctx:        ctx,
		bareDomain: bareDomain,
		Server: &http.Server{
			Handler: handler,
			Addr:    listenAddr,
		},
	}
}

func (s *Server) Listen() error {
	logging.Default().WithFields(logging.Fields{
		"listen_address": s.Server.Addr,
	}).Info("started S3 Gateway server")
	return s.Server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	simulator.ShutdownRecorder()
	s.Server.SetKeepAlivesEnabled(false)
	return s.Server.Shutdown(ctx)
}

func getApiErrOrDefault(err error, defaultApiErr gatewayerrors.APIErrorCode) gatewayerrors.APIError {
	apiError, ok := err.(*gatewayerrors.APIErrorCode)
	if ok {
		return apiError.ToAPIErr()
	} else {
		return defaultApiErr.ToAPIErr()
	}
}

func authenticateOperation(s *ServerContext, writer http.ResponseWriter, request *http.Request, action permissions.Action) *operations.AuthenticatedOperation {
	o := &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         s.region,
		FQDN:           s.bareDomain,

		Index:      s.meta,
		BlockStore: s.blockStore,
		Auth:       s.authService,
		Incr:       func(action string) { s.stats.Collect("s3_gateway", action) },
	}
	// authenticate
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticator(request),
		sig.NewV2SigAuthenticator(request))

	authContext, err := authenticator.Parse()
	if err != nil {
		o.Log().WithError(err).Warn("failed to parse signature")
		o.EncodeError(getApiErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}
	creds, err := s.authService.GetCredentials(authContext.GetAccessKeyId())
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyId()).Warn("error getting access key")
			o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		} else {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyId()).Warn("could not find access key")
			o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrAccessDenied))
		}
		return nil
	}

	err = authenticator.Verify(creds, s.bareDomain)
	if err != nil {
		o.Log().WithError(err).WithFields(logging.Fields{
			"key":           authContext.GetAccessKeyId(),
			"authenticator": authenticator,
		}).Warn("error verifying credentials for key")
		o.EncodeError(getApiErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}

	user, err := s.authService.GetUserById(creds.UserId)
	if err != nil {
		o.Log().WithError(err).WithFields(logging.Fields{
			"key":           authContext.GetAccessKeyId(),
			"authenticator": authenticator,
		}).Warn("could not get user for credentials key")
		o.EncodeError(getApiErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}

	// we are verified!
	op := &operations.AuthenticatedOperation{
		Operation: o,
		SubjectId: user.DisplayName,
	}

	// interpolate arn string
	arn := action.Arn

	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserDisplayName: op.SubjectId,
		Permission:      action.Permission,
		SubjectARN:      arn,
	})
	if err != nil {
		o.Log().WithError(err).Error("failed to authorize")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return nil
	}

	if authResp.Error != nil || !authResp.Allowed {
		o.Log().WithError(authResp.Error).WithField("key", authContext.GetAccessKeyId()).Warn("no permission")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrAccessDenied))
		return nil
	}

	// authentication and authorization complete!
	return op
}

func OperationHandler(ctx *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action("", "", "")
		authOp := authenticateOperation(ctx.WithContext(request.Context()), writer, request, action)
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	})
}

func RepoOperationHandler(ctx *ServerContext, repoId string, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action(repoId, "", "")
		authOp := authenticateOperation(ctx.WithContext(request.Context()), writer, request, action)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Index.GetRepo(repoId)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoId).Warn("the specified repo does not exist")
			authOp.EncodeError(gatewayerrors.ErrNoSuchBucket.ToAPIErr())
			return
		}
		if err != nil {
			authOp.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		// run callback
		repoOperation := &operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repo:                   repo,
		}
		repoOperation.AddLogFields(logging.Fields{
			"repository": repo.Id,
		})
		handler.Handle(repoOperation)
	})
}

func PathOperationHandler(ctx *ServerContext, repoId, refId, path string, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action(repoId, refId, path)
		authOp := authenticateOperation(ctx.WithContext(request.Context()), writer, request, action)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Index.GetRepo(repoId)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoId).Warn("the specified repo does not exist")
			authOp.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchBucket))
			return
		}
		if err != nil {
			authOp.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}

		// run callback
		operation := &operations.PathOperation{
			RefOperation: &operations.RefOperation{
				RepoOperation: &operations.RepoOperation{
					AuthenticatedOperation: authOp,
					Repo:                   repo,
				},
				Ref: refId,
			},
			Path: path,
		}
		operation.AddLogFields(logging.Fields{
			"repository": repo.Id,
			"ref":        refId,
			"path":       path,
		})
		handler.Handle(operation)
	})
}

func unsupportedOperationHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		o := &operations.Operation{
			Request:        request,
			ResponseWriter: writer,
		}
		o.EncodeError(gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}

func notFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}
