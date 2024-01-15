package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const groupCreatedTemplate = `{{ "Group created successfully." | green }}
ID: {{ .Id | bold }}
Creation Date: {{  .CreationDate |date }}
`

var authGroupsCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a group",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		resp, err := clt.CreateGroupWithResponse(cmd.Context(), apigen.CreateGroupJSONRequestBody{
			Name: id,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		group := resp.JSON201
		Write(groupCreatedTemplate, group)
	},
}

//nolint:gochecknoinits
func init() {
	authGroupsCreateCmd.Flags().String("id", "", "Group identifier")
	_ = authGroupsCreateCmd.MarkFlagRequired("id")

	authGroupsCmd.AddCommand(authGroupsCreateCmd)
}
