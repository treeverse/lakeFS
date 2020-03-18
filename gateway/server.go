package gateway

import (
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/gateway/utils"
	"github.com/treeverse/lakefs/index"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/sig"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

type ServerContext struct {
	region           string
	bareDomain       string
	meta             index.Index
	multipartManager index.MultipartManager
	blockStore       block.Adapter
	authService      utils.GatewayService
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
	authService utils.GatewayService,
	multipartManager index.MultipartManager,
	listenAddr, bareDomain string,
) *Server {

	ctx := &ServerContext{
		meta:             meta,
		region:           region,
		bareDomain:       bareDomain,
		blockStore:       blockStore,
		authService:      authService,
		multipartManager: multipartManager,
	}

	// setup routes
	var handler http.Handler
	handler = &Handler{
		BareDomain:         bareDomain,
		ctx:                ctx,
		NotFoundHandler:    http.HandlerFunc(notFound),
		ServerErrorHandler: nil,
	}
	//handler = utils.RegisterRecorder(
	//	httputil.LoggingMiddleWare(
	//		"X-Amz-Request-Id", "s3_gateway", handler,
	//	),
	//)

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
	return s.Server.ListenAndServe()
}

func authenticateOperation(s *ServerContext, writer http.ResponseWriter, request *http.Request, action permissions.Action) *operations.AuthenticatedOperation {
	o := &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         s.region,
		FQDN:           s.bareDomain,

		Index:            s.meta,
		MultipartManager: s.multipartManager,
		BlockStore:       s.blockStore,
		Auth:             s.authService,
	}
	// authenticate
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticatior(request),
		sig.NewV2SigAuthenticator(request))

	authContext, err := authenticator.Parse()
	if err != nil {
		o.Log().WithError(err).WithFields(log.Fields{
			"key": authContext.GetAccessKeyId(),
		}).Warn("error parsing signature")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}
	creds, err := s.authService.GetAPICredentials(authContext.GetAccessKeyId())
	if err != nil {
		if !xerrors.Is(err, db.ErrNotFound) {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyId()).Warn("error getting access key")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		} else {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyId()).Warn("could not find access key")
			o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		}
		return nil
	}

	err = authenticator.Verify(creds, s.bareDomain)
	if err != nil {
		o.Log().WithError(err).WithFields(log.Fields{
			"key":           authContext.GetAccessKeyId(),
			"authenticator": authenticator,
		}).Warn("error verifying credentials for key")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	// we are verified!
	op := &operations.AuthenticatedOperation{
		Operation:   o,
		SubjectId:   creds.GetEntityId(),
		SubjectType: creds.GetCredentialType(),
	}

	// interpolate arn string
	arn := action.Arn

	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserID:     op.SubjectId,
		Permission: action.Permission,
		SubjectARN: arn,
	})
	if err != nil {
		o.Log().WithError(err).Error("failed to authorize")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return nil
	}

	if authResp.Error != nil || !authResp.Allowed {
		o.Log().WithError(authResp.Error).WithField("key", authContext.GetAccessKeyId()).Warn("no permission")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrAccessDenied))
		return nil
	}

	// authentication and authorization complete!
	return op
}

func OperationHandler(ctx *ServerContext, handler operations.AuthenticatedOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action("", "", "")
		authOp := authenticateOperation(ctx, writer, request, action)
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	}
}

func RepoOperationHandler(ctx *ServerContext, repoId string, handler operations.RepoOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action(repoId, "", "")
		authOp := authenticateOperation(ctx, writer, request, action)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Index.GetRepo(repoId)
		if xerrors.Is(err, db.ErrNotFound) {
			log.WithFields(log.Fields{
				"repo": repoId,
			}).Warn("the specified repo does not exist")
			authOp.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
			return
		} else if err != nil {
			authOp.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}
		// run callback
		handler.Handle(&operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repo:                   repo,
		})
	}
}

func PathOperationHandler(ctx *ServerContext, repoId, refId, path string, handler operations.PathOperationHandler) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		action := handler.Action(repoId, refId, path)
		authOp := authenticateOperation(ctx, writer, request, action)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Index.GetRepo(repoId)
		if xerrors.Is(err, db.ErrNotFound) {
			log.WithFields(log.Fields{
				"repo": repoId,
			}).Warn("the specified repo does not exist")
			authOp.EncodeError(errors.Codes.ToAPIErr(errors.ErrNoSuchBucket))
			return
		} else if err != nil {
			authOp.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}

		// run callback
		handler.Handle(&operations.PathOperation{
			RefOperation: &operations.RefOperation{
				RepoOperation: &operations.RepoOperation{
					AuthenticatedOperation: authOp,
					Repo:                   repo,
				},
				Ref: refId,
			},
			Path: path,
		})
	}
}

func unsupportedOperationHandler() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		o := &operations.Operation{
			Request:        request,
			ResponseWriter: writer,
		}
		o.EncodeError(errors.Codes.ToAPIErr(errors.ERRLakeFSNotSupported))
		return
	}
}

func notFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	fmt.Print("URL NOT FOUND\n", r.Method, r.Host)
}
func notAllowed(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
	fmt.Print("METHOD NOT ALLOWED\n", r.Method, r.Host)
}
