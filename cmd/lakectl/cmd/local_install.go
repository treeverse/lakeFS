package cmd

import (
	"os"
	"path"
	"path/filepath"
	"runtime"

	"github.com/mitchellh/go-homedir"

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

var installGitPluginCmd = &cobra.Command{
	Use:   "install-git-plugin <directory>",
	Short: "set up `git data` (directory must exist and be in $PATH)",
	Long: "Add a symlink to lakectl named `git-data`.\n" +
		"This allows calling `git data` and having it act as the `lakectl local` command\n" +
		"(as long as the symlink is within the executing users' $PATH environment variable",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		installDir, err := homedir.Expand(args[0])
		if err != nil {
			DieFmt("could not get directory path %s: %s\n", args[0], err.Error())
		}
		info, err := os.Stat(installDir)
		if err != nil {
			DieFmt("could not check directory %s: %s\n", installDir, err.Error())
		}
		if !info.IsDir() {
			DieFmt("%s: not a directory.\n", installDir)
		}

		fullPath := path.Join(installDir, "git-data")
		if runtime.GOOS == "windows" {
			fullPath += ".exe"
		}
		err = os.Symlink(currentExecutable(), fullPath)
		if err != nil {
			DieFmt("could not create link %s: %s\n", fullPath, err.Error())
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(installGitPluginCmd)
}
