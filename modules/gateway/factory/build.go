package factory

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

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
