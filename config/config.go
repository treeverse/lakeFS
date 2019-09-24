package config

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// IndexerConfiguration holds the root of the indexer instance configuration
type IndexerConfiguration struct {
	ListenAddress string
	Database      fdb.Database
}
