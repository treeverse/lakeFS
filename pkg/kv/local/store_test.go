package local_test

import (
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/local"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"io/ioutil"
	"os"
	"testing"
)

func TestLocalKV(t *testing.T) {
	dir, err := ioutil.TempDir("", "local_kv_testing_*")
	if err != nil {
		t.Fatalf("could not created temp dir %s: %v", dir, err)
	}

	kvtest.TestDriver(t, local.DriverName, kvparams.KV{
		Type: local.DriverName,
		Local: &kvparams.Local{
			DirectoryPath:  dir,
			DisableLogging: true,
		},
	})

	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatalf("could not remove temporary dir %s", dir)
		}
	})
}
