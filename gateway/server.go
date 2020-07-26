package gateway

import (
	"context"
	"errors"
	"net/http"

	dedup2 "github.com/treeverse/lakefs/dedup"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
)

type ServerContext struct {
	ctx         context.Context
	region      string
	bareDomain  string
	cataloger   catalog.Cataloger
	blockStore  block.Adapter
	authService simulator.GatewayAuthService
	stats       stats.Collector
	dedup       *dedup2.Cleaner
}

func (c *ServerContext) WithContext(ctx context.Context) *ServerContext {
	return &ServerContext{
		ctx:         ctx,
		region:      c.region,
		bareDomain:  c.bareDomain,
		cataloger:   c.cataloger,
		blockStore:  c.blockStore.WithContext(ctx),
		authService: c.authService,
		stats:       c.stats,
		dedup:       c.dedup,
	}
}

type Server struct {
	Server *http.Server
}

func NewHandler(
	region string,
	cataloger catalog.Cataloger,
	blockStore block.Adapter,
	authService simulator.GatewayAuthService,
	bareDomain string,
	stats stats.Collector,
	dedup *dedup2.Cleaner,
) http.Handler {
	sc := &ServerContext{
		ctx:         context.Background(),
		cataloger:   cataloger,
		region:      region,
		bareDomain:  bareDomain,
		blockStore:  blockStore,
		authService: authService,
		stats:       stats,
		dedup:       dedup,
	}

	// setup routes
	var handler http.Handler
	handler = &Handler{
		BareDomain:         bareDomain,
		sc:                 sc,
		NotFoundHandler:    http.HandlerFunc(notFound),
		ServerErrorHandler: nil,
	}
	handler = simulator.RegisterRecorder(httputil.LoggingMiddleware(
		"X-Amz-Request-Id", logging.Fields{"service_name": "s3_gateway"}, handler,
	), authService, region, bareDomain)

	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomain,
		"s3_region":      region,
	}).Info("initialized S3 Gateway handler")

	return handler
}

func getApiErrOrDefault(err error, defaultApiErr gatewayerrors.APIErrorCode) gatewayerrors.APIError {
	apiError, ok := err.(*gatewayerrors.APIErrorCode)
	if ok {
		return apiError.ToAPIErr()
	} else {
		return defaultApiErr.ToAPIErr()
	}
}

func authenticateOperation(s *ServerContext, writer http.ResponseWriter, request *http.Request, perms []permissions.Permission) *operations.AuthenticatedOperation {
	o := &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         s.region,
		FQDN:           s.bareDomain,
		Cataloger:      s.cataloger,
		BlockStore:     s.blockStore,
		Auth:           s.authService,
		Incr: func(action string) {
			logging.FromContext(request.Context()).
				WithField("action", action).
				WithField("message_type", "action").
				Debug("performing S3 action")
			s.stats.CollectEvent("s3_gateway", action)
		},
		DedupCleaner: s.dedup,
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
		Principal: user.DisplayName,
	}

	op.AddLogFields(logging.Fields{"user": user.DisplayName})

	if perms == nil {
		// no special permissions required, no need to authorize (used for delete-objects, where permissions are checked separately)
		return op
	}
	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserDisplayName:     op.Principal,
		RequiredPermissions: perms,
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

func operation(sc *ServerContext, writer http.ResponseWriter, request *http.Request) *operations.Operation {
	return &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         sc.region,
		FQDN:           sc.bareDomain,
		Cataloger:      sc.cataloger,
		BlockStore:     sc.blockStore,
		Auth:           sc.authService,
		Incr: func(action string) {
			logging.FromContext(request.Context()).
				WithField("action", action).
				WithField("message_type", "action").
				Debug("performing S3 action")
			sc.stats.CollectEvent("s3_gateway", action)
		},
		DedupCleaner: sc.dedup,
	}
}

func OperationHandler(sc *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	})
}

func RepoOperationHandler(sc *ServerContext, repoID string, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request, repoID)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Cataloger.GetRepository(sc.ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoID).Warn("the specified repo does not exist")
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
			Repository:             repo,
		}
		repoOperation.AddLogFields(logging.Fields{
			"repository": repo.Name,
		})
		handler.Handle(repoOperation)
	})
}

func PathOperationHandler(sc *ServerContext, repoID, refID, path string, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request, repoID, refID, path)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Cataloger.GetRepository(sc.ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoID).Warn("the specified repo does not exist")
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
					Repository:             repo,
				},
				Reference: refID,
			},
			Path: path,
		}
		operation.AddLogFields(logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
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
