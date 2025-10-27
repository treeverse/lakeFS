package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authGroupsDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		resp, err := clt.DeleteGroupWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Println("Group deleted successfully")
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsDeleteCmd.Flags().String("id", "", "Group identifier")
	_ = authGroupsDeleteCmd.MarkFlagRequired("id")

	authGroupsCmd.AddCommand(authGroupsDeleteCmd)
}
