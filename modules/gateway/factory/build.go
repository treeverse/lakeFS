package factory

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// MiddlewareFactory creates a new middleware that will be applied to s3Gateway handler
// the middleware will be applied right after the OperationLookupHandler,
// so the order of the middlewares is (from innermost to outermost):
// 1. LoggingMiddleware - logs request and response details
// 2. OperationLookupHandler - identifies S3 operation type
// 3. --> THE CREATED GATEWAY MIDDLEWARE APPLIED HERE <--
// 4. EnrichWithRepositoryOrFallback - resolves repository context
// 5. EnrichWithParts - adds multipart upload context
// 6. AuthenticationHandler - validates credentials and user
// 7. MetricsMiddleware - request metrics collection
// 8. EnrichWithOperation - populates operation context

type MiddlewareFactory interface {
	Build() func(http.Handler) http.Handler
}

func BuildMiddleware(ctx context.Context, cfg config.Config, logger logging.Logger) (MiddlewareFactory, error) {
	return &noOpMiddlewareFactory{}, nil
}

type noOpMiddlewareFactory struct{}

func (f *noOpMiddlewareFactory) Build() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return next
	}
}
