package application

import (
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

type LakeFsCmdContext struct {
	cfg    *config.Config
	logger logging.Logger
}

func NewLakeFSCmdContext(cfg *config.Config, logger logging.Logger) LakeFsCmdContext {
	return LakeFsCmdContext{
		cfg,
		logger,
	}
}
