package factory

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// MiddlewareFactory creates a new middleware that will be applied to s3Gateway handler
// the middleware will be applied right after the LoggingMiddleware,
// so the order of the middlewares is (from innermost to outermost):
// 1. EnrichWithOperation - populates operation context
// 2. MetricsMiddleware - request metrics collection
// 3. AuthenticationHandler - validates credentials and user
// 4. EnrichWithParts - adds multipart upload context
// 5. EnrichWithRepositoryOrFallback - resolves repository context
// 6. --> THE CREATED GATEWAY MIDDLEWARE APPLIED HERE <--
// 7. LoggingMiddleware - logs request and response details

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
