package factory

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

// MiddlewareFactory creates a new middleware that will be applied to s3Gatway handler
// the middleware will be applied as the outermost layer.
// At this point, the handler chain includes (from innermost to outermost):
// 1. Operation handlers (PUT/GET/etc.)
// 2. OperationLookupHandler - identifies S3 operation type
// 3. EnrichWithRepositoryOrFallback - resolves repository context
// 4. EnrichWithParts - adds multipart upload context
// 5. AuthenticationHandler - validates credentials and user
// 6. MetricsMiddleware - request metrics collection
// 7. EnrichWithOperation - populates operation context
// 8. --> THE CREATED GATEWAY MIDDLEWARE APPLIED HERE <--
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
