package cmd

import (
	"io/ioutil"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/cmdutils"
)

const actionsValidateRequiredArgs = 1

var actionsValidateCmd = &cobra.Command{
	Use:     "validate",
	Short:   "Validate action file",
	Long:    `Tries to parse the input action file as lakeFS action file`,
	Example: "lakectl actions validate <path>",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(actionsValidateRequiredArgs),
	),
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		reader := OpenByPath(file)
		defer func() { _ = reader.Close() }()

		bytes, err := ioutil.ReadAll(reader)
		if err != nil {
			DieErr(err)
		}

		if _, err := actions.ParseAction(bytes); err != nil {
			DieErr(err)
		}
		Fmt("File validated successfully: '%s'\n", file)
	},
}

//nolint:gochecknoinits
func init() {
	actionsCmd.AddCommand(actionsValidateCmd)
}
