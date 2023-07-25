package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var tagShowCmd = &cobra.Command{
	Use:               "show <tag uri>",
	Short:             "Show tag's commit reference",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		u := MustParseRefURI("tag", args[0])
		fmt.Printf("Tag ref: %s\n", []interface{}{u.String()}...)

		ctx := cmd.Context()
		resp, err := client.GetTagWithResponse(ctx, u.Repository, u.Ref)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		fmt.Printf("%s %s\n", []interface{}{resp.JSON200.Id, resp.JSON200.CommitId}...)
	},
}

//nolint:gochecknoinits
func init() {
	tagCmd.AddCommand(tagShowCmd)
}
