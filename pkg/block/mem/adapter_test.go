package mem_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/mem"
)

func TestMemAdapter(t *testing.T) {
	adapter := mem.New(context.Background())
	blocktest.AdapterTest(t, adapter, "mem://test", "mem://external")
}
