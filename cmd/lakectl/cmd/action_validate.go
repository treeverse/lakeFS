package cmd

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/cmdutils"
)

const actionsValidateRequiredArgs = 1

var actionsValidateCmd = &cobra.Command{
	Use:     "validate",
	Short:   "Validate action file",
	Long:    `Tries to parse the input action file as lakeFS action file`,
	Example: "lakectl actions validate <path>",
	Args:    cmdutils.ValidationChain(cobra.ExactArgs(actionsValidateRequiredArgs)),
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		reader := utils.OpenByPath(file)
		defer func() { _ = reader.Close() }()

		bytes, err := io.ReadAll(reader)
		if err != nil {
			utils.DieErr(err)
		}

		if _, err := actions.ParseAction(bytes); err != nil {
			utils.DieErr(err)
		}
		utils.Fmt("File validated successfully: '%s'\n", file)
	},
}

//nolint:gochecknoinits
func init() {
	actionsCmd.AddCommand(actionsValidateCmd)
}
