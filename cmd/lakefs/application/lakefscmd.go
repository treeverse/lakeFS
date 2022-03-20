package application

import (
	"context"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

type LakeFsCmd struct {
	ctx    context.Context
	cfg    *config.Config
	logger logging.Logger
}

func NewLakeFsCmd(ctx context.Context, cfg *config.Config, logger logging.Logger) LakeFsCmd {
	return LakeFsCmd{
		ctx,
		cfg,
		logger,
	}
}
