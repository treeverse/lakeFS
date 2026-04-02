package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var errExpectedKV = errors.New("expected <key>=<value>")

var fsUpdateUserMetadataCmd = &cobra.Command{
	Use:               "update-metadata <path URI>",
	Short:             "Update user metadata on the specified URI",
	Hidden:            true,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		pathURI := MustParsePathURI("path URI", args[0])
		userMetadataKVs, err := cmd.Flags().GetStringSlice("metadata")
		if err != nil {
			DieErr(err)
		}
		userMetadata, err := makeMap(userMetadataKVs)
		if err != nil {
			DieErr(err)
		}

		body := apigen.UpdateObjectUserMetadataJSONRequestBody{
			Set: apigen.ObjectUserMetadata{
				AdditionalProperties: userMetadata,
			},
		}

		client := getClient()

		ctx := cmd.Context()
		resp, err := client.UpdateObjectUserMetadata(
			ctx, pathURI.Repository, pathURI.Ref,
			&apigen.UpdateObjectUserMetadataParams{Path: *pathURI.Path},
			body,
		)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	},
}

// makeMap converts a slice of strings of the form KEY=VALUE into a map.
//
//nolint:mnd
func makeMap(kvs []string) (map[string]string, error) {
	ret := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		s := strings.SplitN(kv, "=", 2)
		if len(s) < 2 {
			return nil, fmt.Errorf("%w, got %s", errExpectedKV, kv)
		}
		k := s[0]
		v := s[1]
		ret[k] = v
	}
	return ret, nil
}

//nolint:gochecknoinits
func init() {
	fsUpdateUserMetadataCmd.Flags().StringSliceP("metadata", "", nil, "Metadata to set, in the form key1=value1,key2=value2")
	fsCmd.AddCommand(fsUpdateUserMetadataCmd)
}
