package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestMemKV(t *testing.T) {
	kvtest.TestDriver(t, mem.DriverName, kvparams.Config{})
}
