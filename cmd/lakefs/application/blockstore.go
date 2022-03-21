package application

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/stats"
	"io"
	"os"
)

type BlockStore struct {
	blockAdapter      block.Adapter
	blockStoreType    string
	metadata          *stats.Metadata
	bufferedCollector *stats.BufferedCollector
}

func NewBlockStore(lakeFsCmd LakeFsCmdContext, authService *AuthService, cloudMetadataProvider cloud.MetadataProvider) (*BlockStore, error) {
	blockstoreType := lakeFsCmd.cfg.GetBlockstoreType()
	if blockstoreType == "local" || blockstoreType == "mem" {
		printLocalWarning(os.Stderr, blockstoreType)
		lakeFsCmd.logger.WithField("adapter_type", blockstoreType).
			Error("Block adapter NOT SUPPORTED for production use")
	}
	metadata := stats.NewMetadata(lakeFsCmd.ctx, lakeFsCmd.logger, blockstoreType, authService.authMetadataManager, cloudMetadataProvider)

	bufferedCollector := stats.NewBufferedCollector(metadata.InstallationID, lakeFsCmd.cfg)
	// init block store
	blockAdapter, err := factory.BuildBlockAdapter(lakeFsCmd.ctx, bufferedCollector, lakeFsCmd.cfg)
	if err != nil {
		return nil, err
	}
	bufferedCollector.SetRuntimeCollector(blockAdapter.RuntimeStats)
	// send metadata
	bufferedCollector.CollectMetadata(metadata)
	return &BlockStore{
		blockAdapter,
		blockstoreType,
		metadata,
		bufferedCollector,
	}, nil
}

func (blockStore BlockStore) CollectRun() {
	blockStore.bufferedCollector.CollectEvent("global", "run")
}
func (blockStore BlockStore) RunCollector(ctx context.Context) {
	blockStore.bufferedCollector.Run(ctx)
}
func (blockStore BlockStore) CollectionChannel() <-chan bool {
	return blockStore.bufferedCollector.Done()
}

func (blockStore BlockStore) InstallationID() string {
	return blockStore.metadata.InstallationID
}

var localWarningBanner = `
WARNING!

Using the "%s" block adapter.  This is suitable only for testing, but not
for production.
`

func printLocalWarning(w io.Writer, adapter string) {
	_, _ = fmt.Fprintf(w, localWarningBanner, adapter)
}
