package gateway_test

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/dedup"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/multiparts"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
)

func setupTest(t *testing.T, method, target string, body io.Reader) *http.Response {
	h, _ := getBasicHandler(t, &simulator.PlayBackMockConf{
		BareDomain:      "example.com",
		AccessKeyID:     "AKIAIO5FODNN7EXAMPLE",
		AccessSecretKey: "MockAccessSecretKey",
		UserID:          1,
		Region:          "MockRegion",
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, body)
	req.Header["Content-Type"] = []string{"text/tab - separated - values"}
	req.Header["X-Amz-Content-Sha256"] = []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	req.Header["X-Amz-Date"] = []string{"20200517T093907Z"}
	req.Header["Host"] = []string{"host.domain.com"}
	req.Header["Authorization"] = []string{"AWS4-HMAC-SHA256 Credential=AKIAIO5FODNN7EXAMPLE/20200517/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=cdb193f2140d1d0c093adc7aba9a62bc3c75f84b117100888553115900f39223"}
	h.ServeHTTP(rr, req)
	return rr.Result()
}

func TestPathWithTrailingSlash(t *testing.T) {
	result := setupTest(t, http.MethodHead, "/example/", nil)
	assert.Equal(t, 200, result.StatusCode)
	bytes, err := ioutil.ReadAll(result.Body)
	assert.NoError(t, err)
	assert.Len(t, bytes, 0)
	assert.Contains(t, result.Header, "X-Amz-Request-Id")
}

func TestServerContext_WithContext(t *testing.T) {
	type fields struct {
		ctx               context.Context
		region            string
		bareDomain        string
		cataloger         catalog.Cataloger
		multipartsTracker multiparts.Tracker
		blockStore        block.Adapter
		authService       simulator.GatewayAuthService
		stats             stats.Collector
		dedupCleaner      *dedup.Cleaner
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ServerContext
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ServerContext{
				ctx:               tt.fields.ctx,
				region:            tt.fields.region,
				bareDomain:        tt.fields.bareDomain,
				cataloger:         tt.fields.cataloger,
				multipartsTracker: tt.fields.multipartsTracker,
				blockStore:        tt.fields.blockStore,
				authService:       tt.fields.authService,
				stats:             tt.fields.stats,
				dedupCleaner:      tt.fields.dedupCleaner,
			}
			if got := c.WithContext(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ServerContext.WithContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewHandler(t *testing.T) {
	type args struct {
		region            string
		cataloger         catalog.Cataloger
		multipartsTracker multiparts.Tracker
		blockStore        block.Adapter
		authService       simulator.GatewayAuthService
		bareDomain        string
		stats             stats.Collector
		dedupCleaner      *dedup.Cleaner
	}
	tests := []struct {
		name string
		args args
		want http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewHandler(tt.args.region, tt.args.cataloger, tt.args.multipartsTracker, tt.args.blockStore, tt.args.authService, tt.args.bareDomain, tt.args.stats, tt.args.dedupCleaner); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getAPIErrOrDefault(t *testing.T) {
	type args struct {
		err           error
		defaultAPIErr gatewayerrors.APIErrorCode
	}
	tests := []struct {
		name string
		args args
		want gatewayerrors.APIError
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getAPIErrOrDefault(tt.args.err, tt.args.defaultAPIErr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAPIErrOrDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_authenticateOperation(t *testing.T) {
	type args struct {
		s       *ServerContext
		writer  http.ResponseWriter
		request *http.Request
		perms   []permissions.Permission
	}
	tests := []struct {
		name string
		args args
		want *operations.AuthenticatedOperation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := authenticateOperation(tt.args.s, tt.args.writer, tt.args.request, tt.args.perms); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("authenticateOperation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_operation(t *testing.T) {
	type args struct {
		sc      *ServerContext
		writer  http.ResponseWriter
		request *http.Request
	}
	tests := []struct {
		name string
		args args
		want *operations.Operation
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := operation(tt.args.sc, tt.args.writer, tt.args.request); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("operation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOperationHandler(t *testing.T) {
	type args struct {
		sc      *ServerContext
		handler operations.AuthenticatedOperationHandler
	}
	tests := []struct {
		name string
		args args
		want http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OperationHandler(tt.args.sc, tt.args.handler); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OperationHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepoOperationHandler(t *testing.T) {
	type args struct {
		sc      *ServerContext
		repoID  string
		handler operations.RepoOperationHandler
	}
	tests := []struct {
		name string
		args args
		want http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RepoOperationHandler(tt.args.sc, tt.args.repoID, tt.args.handler); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RepoOperationHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPathOperationHandler(t *testing.T) {
	type args struct {
		sc      *ServerContext
		repoID  string
		refID   string
		path    string
		handler operations.PathOperationHandler
	}
	tests := []struct {
		name string
		args args
		want http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathOperationHandler(tt.args.sc, tt.args.repoID, tt.args.refID, tt.args.path, tt.args.handler); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PathOperationHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_unsupportedOperationHandler(t *testing.T) {
	tests := []struct {
		name string
		want http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unsupportedOperationHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unsupportedOperationHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_notFound(t *testing.T) {
	type args struct {
		w   http.ResponseWriter
		in1 *http.Request
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notFound(tt.args.w, tt.args.in1)
		})
	}
}

func Test_handler_ServeHTTP(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			h.ServeHTTP(tt.args.w, tt.args.r)
		})
	}
}

func Test_handler_servePathBased(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			if got := h.servePathBased(tt.args.r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler.servePathBased() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_handler_serveVirtualHost(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			if got := h.serveVirtualHost(tt.args.r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler.serveVirtualHost() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_handler_pathBasedHandler(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		method     string
		repository string
		ref        string
		path       string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			if got := h.pathBasedHandler(tt.args.method, tt.args.repository, tt.args.ref, tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler.pathBasedHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_handler_repositoryBasedHandlerIfValid(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		method     string
		repository string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			if got := h.repositoryBasedHandlerIfValid(tt.args.method, tt.args.repository); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler.repositoryBasedHandlerIfValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_handler_repositoryBasedHandler(t *testing.T) {
	type fields struct {
		BareDomain         string
		sc                 *ServerContext
		operationID        string
		NotFoundHandler    http.Handler
		ServerErrorHandler http.Handler
	}
	type args struct {
		method     string
		repository string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   http.Handler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &handler{
				BareDomain:         tt.fields.BareDomain,
				sc:                 tt.fields.sc,
				operationID:        tt.fields.operationID,
				NotFoundHandler:    tt.fields.NotFoundHandler,
				ServerErrorHandler: tt.fields.ServerErrorHandler,
			}
			if got := h.repositoryBasedHandler(tt.args.method, tt.args.repository); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler.repositoryBasedHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitFirst(t *testing.T) {
	type args struct {
		pth   string
		parts int
	}
	tests := []struct {
		name  string
		args  args
		want  []string
		want1 bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := SplitFirst(tt.args.pth, tt.args.parts)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SplitFirst() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("SplitFirst() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
