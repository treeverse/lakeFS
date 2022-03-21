package application

import (
	"context"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

type LakeFsCmdContext struct {
	ctx    context.Context
	cfg    *config.Config
	logger logging.Logger
}

func NewLakeFsCmdContext(ctx context.Context, cfg *config.Config, logger logging.Logger) LakeFsCmdContext {
	return LakeFsCmdContext{
		ctx,
		cfg,
		logger,
	}
}
