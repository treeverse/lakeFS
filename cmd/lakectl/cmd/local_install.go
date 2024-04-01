package cmd

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func currentExecutable() string {
	ex, err := os.Executable()
	if err != nil {
		DieErr(err)
	}
	absolute, err := filepath.Abs(ex)
	if err != nil {
		DieErr(err)
	}
	return absolute
}

var localInstallGitPluginCmd = &cobra.Command{
	Use:   "install-git-plugin <directory>",
	Short: "set up `git data` (directory must exist and be in $PATH)",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		installDir := args[0]
		if strings.HasPrefix(installDir, "~/") {
			dirname, _ := os.UserHomeDir()
			installDir = filepath.Join(dirname, installDir[2:])
		}
		info, err := os.Stat(installDir)
		if err != nil {
			DieFmt("could not check directory %s: %s\n", installDir, err.Error())
		}
		if !info.IsDir() {
			DieFmt("%s: not a directory.\n", installDir)
		}
		fullPath := path.Join(installDir, "git-data")
		err = os.Symlink(currentExecutable(), fullPath)
		if err != nil {
			DieFmt("could not create link %s: %s\n", fullPath, err.Error())
		}
	},
}
