package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const externalPrincipalTemplate = `Principal ID: {{ .PrincipalID | bold }}
User ID: {{ .UserID | bold }}
`

var authAWSIAMLookup = &cobra.Command{
	Use:   "lookup",
	Short: "Lookup an external principal (IAM role)",
	Run: func(cmd *cobra.Command, args []string) {
		principalID := Must(cmd.Flags().GetString("principal-id"))
		clt := getClient()

		resp, err := clt.GetExternalPrincipalWithResponse(cmd.Context(), &apigen.GetExternalPrincipalParams{
			PrincipalId: principalID,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		principal := resp.JSON200
		Write(externalPrincipalTemplate, struct {
			PrincipalID string
			UserID      string
		}{
			PrincipalID: principal.Id,
			UserID:      principal.UserId,
		})
	},
}

//nolint:gochecknoinits
func init() {
	authAWSIAMLookup.Flags().String("principal-id", "", "External principal ID (e.g., AWS IAM role ARN)")
	_ = authAWSIAMLookup.MarkFlagRequired("principal-id")

	authUsersAWSIAMCmd.AddCommand(authAWSIAMLookup)
}
