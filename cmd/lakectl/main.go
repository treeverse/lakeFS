package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-ps"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

func isGitSubproc() bool {
	parentProc, err := ps.FindProcess(os.Getppid())
	if err == nil {
		parentName := parentProc.Executable()
		return parentName == "git" || strings.HasPrefix(parentName, "git.")
	}
	return false
}

func main() {
	filename := filepath.Base(os.Args[0])
	if isGitSubproc() {
		// running as a git sub-command (e.g. `git data ...`)
		// this means that our binary is most likely in path and called something like `git-data`.
		baseName := filepath.Base(os.Args[0])
		if strings.HasPrefix(baseName, "git-") {
			baseName = baseName[4:]
		}
		// trim any ".exe" or similar extension
		lastDot := strings.LastIndex(baseName, ".")
		if lastDot != -1 {
			baseName = baseName[:lastDot]
		}
		cmd.LocalExecute(baseName)
		return
	}
	if strings.HasPrefix(filename, "git") {
		cmd.LocalExecute(filename)
		return
	}
	// simply run lakectl
	cmd.Execute()
}
