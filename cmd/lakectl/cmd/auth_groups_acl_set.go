package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

var authGroupsACLSet = &cobra.Command{
	Use:   "set",
	Short: "Set ACL of group",
	Long:  `Set ACL of group. permission will be attached to all repositories.`,
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		permission, _ := cmd.Flags().GetString("permission")

		clt := getClient()

		acl := api.SetGroupACLJSONRequestBody{
			Permission: permission,
		}

		resp, err := clt.SetGroupACL(cmd.Context(), id, acl)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsACLSet.Flags().String("id", "", "Group identifier")
	_ = authGroupsACLSet.MarkFlagRequired("id")
	authGroupsACLSet.Flags().String("permission", "", `Permission, typically one of "Read", "Write", "Super" or "Admin"`)
	_ = authGroupsACLSet.MarkFlagRequired("permission")

	authGroupsACLCmd.AddCommand(authGroupsACLSet)
}
