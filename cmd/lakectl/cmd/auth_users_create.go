package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const userCreatedTemplate = `{{ "User created successfully." | green }}
ID: {{ .Id | bold }}
Creation Date: {{  .CreationDate |date }}
`

var authUsersCreate = &cobra.Command{
	Use:   "create",
	Short: "Create a user",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		resp, err := clt.CreateUserWithResponse(cmd.Context(), apigen.CreateUserJSONRequestBody{
			Id: id,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		user := resp.JSON201
		Write(userCreatedTemplate, user)
	},
}

//nolint:gochecknoinits
func init() {
	authUsersCreate.Flags().String("id", "", "Username")
	_ = authUsersCreate.MarkFlagRequired("id")

	authUsersCmd.AddCommand(authUsersCreate)
}
