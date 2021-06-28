package cmd

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

const (
	gcSetConfigCmdArgs = 1
	gcRulesTemplate    = `Default Retention Days: {{ .DefaultRetentionDays }}
Branch Rules: {{ range $branch := .Branches }}
  - Branch: {{ $branch.BranchId }}
    Retention Days: {{ $branch.RetentionDays }}{{ end }}`
)

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "manage garbage collection configuration",
}

var gcSetConfigCmd = &cobra.Command{
	Use:   "set-config",
	Short: "set the garbage collection configuration JSON",
	Long: `Sets the garbage collection configuration JSON.
Example configuration file:
{
  "default_retention_days": 21
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
		u := MustParseRepoURI("repository", args[0])
		filename := MustString(cmd.Flags().GetString("filename"))
		file, err := ioutil.ReadFile(filename)
		if err != nil {
			DieErr(err)
		}
		var body api.SetGarbageCollectionRulesJSONRequestBody
		err = json.Unmarshal(file, &body)
		if err != nil {
			DieErr(err)
		}
		client := getClient()
		resp, err := client.SetGarbageCollectionRulesWithResponse(cmd.Context(), u.Repository, body)
		DieOnResponseError(resp, err)
		if resp.StatusCode() != http.StatusNoContent {
			Die("Failed to update config", 1)
		}
	},
}

var gcGetConfigCmd = &cobra.Command{
	Use:     "get-config",
	Short:   "show the garbage collection configuration JSON",
	Example: "lakectl gc get-config <repository uri>",
	Args:    cobra.ExactArgs(gcSetConfigCmdArgs),
	Run: func(cmd *cobra.Command, args []string) {
		u := MustParseRepoURI("repository", args[0])
		isJson := MustBool(cmd.Flags().GetBool("json"))
		client := getClient()
		resp, err := client.GetGarbageCollectionRulesWithResponse(cmd.Context(), u.Repository)
		DieOnResponseError(resp, err)
		if isJson {
			Write("{{ . | json }}", resp.JSON200)
		} else {
			Write(gcRulesTemplate, resp.JSON200)
		}
	},
}

//nolint:gochecknoinits
func init() {
	gcSetConfigCmd.Flags().StringP("filename", "f", "", "file containing the GC configuration")
	_ = gcSetConfigCmd.MarkFlagRequired("filename")
	gcGetConfigCmd.Flags().BoolP("json", "p", false, "get rules as JSON")
	rootCmd.AddCommand(gcCmd)
	gcCmd.AddCommand(gcSetConfigCmd)
	gcCmd.AddCommand(gcGetConfigCmd)
}
