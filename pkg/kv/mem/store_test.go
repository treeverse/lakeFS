package mem_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestMemKV(t *testing.T) {
	kvtest.TestDriver(t, "mem", "")
}
