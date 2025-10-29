package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const userInfoTemplate = `User ID:          {{.UserID|yellow}}
Email:            {{if .Email}}{{.Email|blue}}{{end}}
Creation Date:    {{.CreationDate|date}}
`

var identityCmd = &cobra.Command{
	Use:               "identity",
	Short:             "Show identity info",
	Long:              "Show the info of the configured user in lakectl",
	Example:           "lakectl identity",
	Args:              cobra.ExactArgs(0),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		resp, err := client.GetCurrentUserWithResponse(cmd.Context())
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		id := resp.JSON200.User.Id
		CreationDate := resp.JSON200.User.CreationDate
		email := ""
		if resp.JSON200.User.Email != nil {
			email = *resp.JSON200.User.Email
		}

		Write(userInfoTemplate, struct {
			UserID       string
			Email        string
			CreationDate int64
		}{UserID: id, CreationDate: CreationDate, Email: email})

	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(identityCmd)
}
