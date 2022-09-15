package local_test

import (
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/local"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestLocalKV(t *testing.T) {
	dir, err := os.MkdirTemp("", "local_kv_testing_*")
	if err != nil {
		t.Fatalf("could not created temp: %v", err)
	}
	defer os.RemoveAll(dir)

	kvtest.TestDriver(t, local.DriverName, kvparams.KV{
		Type: local.DriverName,
		Local: &kvparams.Local{
			Path:          dir,
			EnableLogging: true,
		},
	})
}
