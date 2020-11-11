package cmd

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

var exportCmd = &cobra.Command{
	Use:    "export",
	Hidden: true,
	Short:  "create and manage continuous export",
	Long:   `Read set and update continuous export configurations and trigger exports`,
}

// exportGetCmd get continuous export configuration for branch
var exportSetCmd = &cobra.Command{
	Use:   "set <branch uri>",
	Short: "set continuous export configuration for branch",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		exportPath, err := cmd.Flags().GetString("export-path")
		if err != nil {
			DieErr(err)
		}
		exportStatusPath, err := cmd.Flags().GetString("export-status-path")
		if err != nil {
			DieErr(err)
		}
		prefixRegex, err := cmd.Flags().GetStringArray("prefix-regex")
		if err != nil {
			DieErr(err)
		}
		config := &models.ContinuousExportConfiguration{
			ExportPath:             strfmt.URI(exportPath),
			ExportStatusPath:       strfmt.URI(exportStatusPath),
			LastKeysInPrefixRegexp: prefixRegex,
		}
		err = client.SetContinuousExport(context.Background(), branchURI.Repository, branchURI.Ref, config)
		if err != nil {
			DieErr(err)
		}
	},
}

var exportConfigurationTemplate = `export configuration for branch "{{.Branch.Ref}}" completed.

Export Path: {{.Configuration.ExportPath|yellow}}
Export status path: {{.Configuration.ExportStatusPath}}
Last Keys In Prefix Regexp: {{.Configuration.LastKeysInPrefixRegexp}}

`

// exportGetCmd get continuous export configuration for branch
var exportGetCmd = &cobra.Command{
	Use:   "get <branch uri>",
	Short: "get continuous export configuration for branch",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		configuration, err := client.GetContinuousExport(context.Background(), branchURI.Repository, branchURI.Ref)

		if err != nil {
			DieErr(err)
		}
		Write(exportConfigurationTemplate, struct {
			Branch        *uri.URI
			Configuration *models.ContinuousExportConfiguration
		}{branchURI, configuration})
	},
}

// exportSetCmd get continuous export configuration for branch
var exportExecuteCmd = &cobra.Command{
	Use:   "run",
	Short: "export requested branch now",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		exportID, err := client.RunExport(context.Background(), branchURI.Repository, branchURI.Ref)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("Export-ID:%s\n", exportID)
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.AddCommand(exportGetCmd)
	exportCmd.AddCommand(exportSetCmd)
	exportCmd.AddCommand(exportExecuteCmd)

	exportSetCmd.Flags().String("export-path", "", "export objects to this path")
	exportSetCmd.Flags().String("export-status-path", "", "write export status object to this path")
	exportSetCmd.Flags().StringArray("prefix-regex", nil, "list of regexps of keys to exported last in each prefix (for signalling)")
}
