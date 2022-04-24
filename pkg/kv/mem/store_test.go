package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestMemKV(t *testing.T) {
	kvtest.TestDriver(t, "mem", "")
}
