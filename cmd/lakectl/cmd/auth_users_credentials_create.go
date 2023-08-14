package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const credentialsCreatedTemplate = `{{ "Credentials created successfully." | green }}
{{ "Access Key ID:" | ljust 18 }} {{ .AccessKeyId | bold }}
{{ "Secret Access Key:" | ljust 18 }} {{  .SecretAccessKey | bold }}

{{ "Keep these somewhere safe since you will not be able to see the secret key again" | yellow }}
`

var authUsersCredentialsCreate = &cobra.Command{
	Use:   "create",
	Short: "Create user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.CreateCredentialsWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		credentials := resp.JSON201
		Write(credentialsCreatedTemplate, credentials)
	},
}

//nolint:gochecknoinits
func init() {
	authUsersCredentialsCreate.Flags().String("id", "", "Username (email for password-based users, default: current user)")

	authUsersCredentials.AddCommand(authUsersCredentialsCreate)
}
