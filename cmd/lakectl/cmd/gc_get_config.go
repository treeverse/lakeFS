package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
)

const gcRulesTemplate = `Default Retention Days: {{ .DefaultRetentionDays }}
Branch Rules: {{ range $branch := .Branches }}
  - Branch: {{ $branch.BranchId }}
    Retention Days: {{ $branch.RetentionDays }}{{ end }}
`

const jsonFlagName = "json"

var gcGetConfigCmd = &cobra.Command{
	Use:               "get-config <repository URI>",
	Short:             "Show the garbage collection policy for this repository",
	Example:           "lakectl gc get-config " + myRepoExample,
	Args:              cobra.ExactArgs(gcSetConfigCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository URI", args[0])
		isJSON := Must(cmd.Flags().GetBool(jsonFlagName))
		client := getClient()
		resp, err := client.GetGCRulesWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		if isJSON {
			Write("{{ . | json }}", resp.JSON200)
		} else {
			Write(gcRulesTemplate, resp.JSON200)
		}
	},
}

//nolint:gochecknoinits
func init() {
	gcGetConfigCmd.Flags().BoolP(jsonFlagName, "p", false, "get rules as JSON")

	gcCmd.AddCommand(gcGetConfigCmd)
}
