package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	gcSetConfigCmdArgs = 1
	gcRulesTemplate    = `Default Retention Days: {{ .DefaultRetentionDays }}
Branch Rules: {{ range $branch := .Branches }}
  - Branch: {{ $branch.BranchId }}
    Retention Days: {{ $branch.RetentionDays }}{{ end }}
`

	filenameFlagName = "filename"
	jsonFlagName     = "json"
)

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "Manage the garbage collection policy",
}

var gcSetConfigCmd = &cobra.Command{
	Use:   "set-config",
	Short: "Set garbage collection policy JSON",
	Long: `Sets the garbage collection policy JSON.
Example configuration file:
{
  "default_retention_days": 21,
  "branches": [
    {
      "branch_id": "main",
      "retention_days": 28
    },
    {
      "branch_id": "dev",
      "retention_days": 14
    }
  ]
}`,
	Example: "lakectl gc set-config <repository uri> -f config.json",
	Args:    cobra.ExactArgs(gcSetConfigCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		u := utils.MustParseRepoURI("repository", args[0])
		filename := utils.MustString(cmd.Flags().GetString(filenameFlagName))
		var reader io.ReadCloser
		var err error
		if filename == "-" {
			reader = os.Stdin
		} else {
			reader, err = os.Open(filename)
			if err != nil {
				utils.DieErr(err)
			}
			defer func() {
				_ = reader.Close()
			}()
		}
		var body api.SetGarbageCollectionRulesJSONRequestBody
		err = json.NewDecoder(reader).Decode(&body)
		if err != nil {
			utils.DieErr(err)
		}
		client := getClient()
		resp, err := client.SetGarbageCollectionRulesWithResponse(cmd.Context(), u.Repository, body)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

var gcDeleteConfigCmd = &cobra.Command{
	Use:               "delete-config",
	Short:             "Deletes the garbage collection policy for the repository",
	Example:           "lakectl gc delete-config <repository uri>",
	Args:              cobra.ExactArgs(gcSetConfigCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := utils.MustParseRepoURI("repository", args[0])
		client := getClient()
		resp, err := client.DeleteGarbageCollectionRulesWithResponse(cmd.Context(), u.Repository)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

var gcGetConfigCmd = &cobra.Command{
	Use:               "get-config",
	Short:             "Show the garbage collection policy for this repository",
	Example:           "lakectl gc get-config <repository uri>",
	Args:              cobra.ExactArgs(gcSetConfigCmdArgs),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		u := utils.MustParseRepoURI("repository", args[0])
		isJSON := utils.MustBool(cmd.Flags().GetBool(jsonFlagName))
		client := getClient()
		resp, err := client.GetGarbageCollectionRulesWithResponse(cmd.Context(), u.Repository)
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			utils.Die("Bad response from server", 1)
		}
		if isJSON {
			utils.Write("{{ . | json }}", resp.JSON200)
		} else {
			utils.Write(gcRulesTemplate, resp.JSON200)
		}
	},
}

//nolint:gochecknoinits
func init() {
	gcSetConfigCmd.Flags().StringP(filenameFlagName, "f", "", "file containing the GC policy as JSON")
	_ = gcSetConfigCmd.MarkFlagRequired(filenameFlagName)
	gcGetConfigCmd.Flags().BoolP(jsonFlagName, "p", false, "get rules as JSON")
	rootCmd.AddCommand(gcCmd)
	gcCmd.AddCommand(gcSetConfigCmd)
	gcCmd.AddCommand(gcGetConfigCmd)
	gcCmd.AddCommand(gcDeleteConfigCmd)
}
