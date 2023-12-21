package cmd

import (
	"fmt"
	"net/http"

	"github.com/go-openapi/swag"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

// lakectl branch reset lakefs://myrepo/main --commit commitId --prefix path --object path
var branchResetCmd = &cobra.Command{
	Use:     "reset <branch URI> [--prefix|--object]",
	Example: "lakectl branch reset " + myRepoExample + "/" + myBranchExample,
	Short:   "Reset uncommitted changes - all of them, or by path",
	Long: `reset changes.  There are four different ways to reset changes:
  1. reset all uncommitted changes - reset lakefs://myrepo/main 
  2. reset uncommitted changes under specific path - reset lakefs://myrepo/main --prefix path
  3. reset uncommitted changes for specific object - reset lakefs://myrepo/main --object path`,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseBranchURI("branch URI", args[0])
		fmt.Println("Branch:", u)
		prefix, err := cmd.Flags().GetString("prefix")
		if err != nil {
			DieErr(err)
		}
		object, err := cmd.Flags().GetString("object")
		if err != nil {
			DieErr(err)
		}
		force := Must(cmd.Flags().GetBool("force"))

		var reset apigen.ResetCreation
		var confirmationMsg string
		switch {
		case len(prefix) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all uncommitted changes from path: %s", prefix)
			reset = apigen.ResetCreation{
				Path:  &prefix,
				Type:  "common_prefix",
				Force: swag.Bool(force),
			}
		case len(object) > 0:
			confirmationMsg = fmt.Sprintf("Are you sure you want to reset all uncommitted changes for object: %s", object)
			reset = apigen.ResetCreation{
				Path:  &object,
				Type:  "object",
				Force: swag.Bool(force),
			}
		default:
			confirmationMsg = "Are you sure you want to reset all uncommitted changes"
			reset = apigen.ResetCreation{
				Type:  "reset",
				Force: swag.Bool(force),
			}
		}

		confirmation, err := Confirm(cmd.Flags(), confirmationMsg)
		if err != nil || !confirmation {
			Die("Reset aborted", 1)
			return
		}
		resp, err := clt.ResetBranchWithResponse(cmd.Context(), u.Repository, u.Ref, apigen.ResetBranchJSONRequestBody(reset))
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(branchResetCmd.Flags())

	branchResetCmd.Flags().String("prefix", "", "prefix of the objects to be reset")
	branchResetCmd.Flags().String("object", "", "path to object to be reset")
	branchResetCmd.Flags().Bool("force", false, "ignore read-only protection on the repository")

	branchCmd.AddCommand(branchResetCmd)
}
