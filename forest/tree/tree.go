package tree

import (
	cache "github.com/treeverse/lakefs/forest/cache_map"
	"github.com/treeverse/lakefs/forest/sstable"
	gr "github.com/treeverse/lakefs/graveler"
)

var minimalKey = gr.Key(nil)

type TreePartType struct {
	PartName sstable.ID `json:"part_name"`
	MaxKey   gr.Key     `json:"max_path"`
}
type TreeType []TreePartType

type TreesRepoType struct {
	TreesMap   cache.CacheMap
	PartManger sstable.Manager
}

const (
	CacheMapSize     = 1000
	CacheTrimSize    = 100
	InitialWeight    = 64
	AdditionalWeight = 16
	TrimFactor       = 1
)

func InitTreesRepository(manager sstable.Manager) *TreesRepoType {
	treesRepository := &TreesRepoType{
		TreesMap:   cache.NewCacheMap(CacheMapSize, CacheTrimSize, InitialWeight, AdditionalWeight, TrimFactor),
		PartManger: manager,
	}
	return treesRepository
}
