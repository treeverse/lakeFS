package handlers

import (
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/index"
)

type ServerContext interface {
	GetRegion() string
	GetIndex() index.Index
	GetMultipartManager() index.MultipartManager
	GetBlockStore() block.Adapter
	GetAuthService() auth.Service
}
