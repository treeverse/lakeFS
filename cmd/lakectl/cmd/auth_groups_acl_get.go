package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const permissionTemplate = "Group {{ .Group }}:{{ .Permission }}\n"

var authGroupsACLGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get ACL of group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.GetGroupACLWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		Write(permissionTemplate, struct{ Group, Permission string }{id, resp.JSON200.Permission})
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsACLGetCmd.Flags().String("id", "", "Group identifier")
	_ = authGroupsACLGetCmd.MarkFlagRequired("id")

	authGroupsACLCmd.AddCommand(authGroupsACLGetCmd)
}
