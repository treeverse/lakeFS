package graveler_test

import (
	"flag"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/logging"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic", false)
	}

	code := m.Run()
	defer os.Exit(code)
}
