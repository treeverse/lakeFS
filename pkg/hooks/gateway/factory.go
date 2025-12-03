package factory

import (
	"context"
	"net/http"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/entensionhooks"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MiddlewareFactory interface {
	Build() func(http.Handler) http.Handler
}

func BuildMiddleware(ctx context.Context, cfg config.Config, logger logging.Logger) (MiddlewareFactory, error) {
	if hook := entensionhooks.Get().GatewayMiddleware; hook != nil {
		mw, err := hook.Build(ctx, cfg, logger)
		if err != nil {
			return nil, err
		}
		return &hookMiddlewareFactory{mw: mw}, nil
	}
	return &noOpMiddlewareFactory{}, nil
}

type noOpMiddlewareFactory struct{}

func (f *noOpMiddlewareFactory) Build() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return next
	}
}

type hookMiddlewareFactory struct {
	mw func(http.Handler) http.Handler
}

func (f *hookMiddlewareFactory) Build() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return f.mw(next)
	}
}
