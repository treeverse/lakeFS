package local_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/local"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestLocalKV(t *testing.T) {
	kvtest.TestDriver(t, local.DriverName, kvparams.KV{
		Type: local.DriverName,
		Local: &kvparams.Local{
			Path:          t.TempDir(),
			EnableLogging: true,
		},
	})
}
