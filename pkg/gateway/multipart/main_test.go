package multipart_test

import (
	"flag"
	"os"
	"testing"

	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}
	code := m.Run()
	defer os.Exit(code)
}
