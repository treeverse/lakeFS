package cmd

import (
	"github.com/spf13/cobra"
)

const usageSummaryTemplate = `Usage report for installation ID: {{.InstallationID}}

{{ .Reports | table }}
`

var usageSummaryCmd = &cobra.Command{
	Use:    "summary",
	Short:  "Summary reports from lakeFS",
	Args:   cobra.NoArgs,
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		ctx := cmd.Context()
		resp, err := client.GetUsageReportSummaryWithResponse(ctx)
		if err != nil {
			DieErr(err)
		}
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

		// convert the summary to data format for the template
		summary := resp.JSON200
		data := struct {
			InstallationID string
			Reports        *Table
		}{
			InstallationID: summary.InstallationId,
			Reports: &Table{
				Headers: []interface{}{
					"Year",
					"Month",
					"Usage",
				},
				Rows: make([][]interface{}, 0, len(summary.Reports)),
			},
		}
		for _, report := range summary.Reports {
			data.Reports.Rows = append(data.Reports.Rows, []interface{}{
				report.Year,
				report.Month,
				report.Count,
			})
		}

		Write(usageSummaryTemplate, data)
	},
}

//nolint:gochecknoinits
func init() {
	usageCmd.AddCommand(usageSummaryCmd)
}
