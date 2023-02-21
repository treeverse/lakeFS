package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

func main() {
	filename := filepath.Base(os.Args[0])
	if strings.HasPrefix(filename, "git") {
		cmd.LocalExecute(filename)
	} else {
		cmd.Execute()
	}
}
