package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/mem"
)

func TestMemAdapter(t *testing.T) {
	adapter := mem.New(t.Context())
	blocktest.AdapterTest(t, adapter, "mem://test", "mem://external")
}
