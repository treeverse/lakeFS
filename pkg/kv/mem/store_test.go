package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestMemKV(t *testing.T) {
	kvtest.DriverTest(t, mem.DriverName, kvparams.Config{})
}
