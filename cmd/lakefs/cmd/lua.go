package cmd

import (
	"fmt"
	"os"

	"github.com/Shopify/go-lua"
	"github.com/spf13/cobra"
	lualibs "github.com/treeverse/lakefs/pkg/actions/lua"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
)

var luaCmd = &cobra.Command{
	Use:    "lua",
	Short:  "Lua related commands for dev/test scripting",
	Hidden: true,
}

var luaRunCmd = &cobra.Command{
	Use:   "run [file.lua]",
	Short: "Run lua code locally for testing. Use stdin when no file is given",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var filename string
		if len(args) > 0 {
			filename = args[0]
		}

		ctx := cmd.Context()
		l := lua.NewStateEx()
		lualibs.OpenSafe(l, ctx, lualibs.OpenSafeConfig{NetHTTPEnabled: true}, os.Stdout)
		luautil.DeepPush(l, args)
		l.SetGlobal("args") // add cmd line args as lua args
		if err := lua.DoFile(l, filename); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(luaCmd)
	luaCmd.AddCommand(luaRunCmd)
}
