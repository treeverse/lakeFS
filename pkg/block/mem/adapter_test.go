package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	adapterTest "github.com/treeverse/lakefs/pkg/block/test"
)

func TestMemAdapter(t *testing.T) {
	var params []func(*mem.Adapter)
	adapterTest.TestAdapter(t, block.BlockstoreTypeMem, params)
}
