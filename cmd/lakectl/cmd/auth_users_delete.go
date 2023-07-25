package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var authUsersDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.DeleteUserWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		fmt.Printf("User deleted successfully\n", []interface{}{}...)
	},
}

//nolint:gochecknoinits
func init() {
	authUsersDelete.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersDelete.MarkFlagRequired("id")

	authUsersCmd.AddCommand(authUsersDelete)
}
