package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const policyCreatedTemplate = `{{ "Policy created successfully." | green }}
` + policyDetailsTemplate

var authPoliciesCreate = &cobra.Command{
	Use:   "create",
	Short: "Create a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id := Must(cmd.Flags().GetString("id"))
		document := Must(cmd.Flags().GetString("statement-document"))
		clt := getClient()

		var err error
		var fp io.ReadCloser
		if document == "-" {
			fp = os.Stdin
		} else {
			fp, err = os.Open(document)
			if err != nil {
				DieFmt("could not open policy document: %v", err)
			}
			defer func() {
				_ = fp.Close()
			}()
		}

		var doc StatementDoc
		err = json.NewDecoder(fp).Decode(&doc)
		if err != nil {
			DieFmt("could not parse statement JSON document: %v", err)
		}
		resp, err := clt.CreatePolicyWithResponse(cmd.Context(), apigen.CreatePolicyJSONRequestBody{
			Id:        id,
			Statement: doc.Statement,
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

		createdPolicy := resp.JSON201
		Write(policyCreatedTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc StatementDoc
		}{
			ID:           createdPolicy.Id,
			CreationDate: *createdPolicy.CreationDate,
			StatementDoc: StatementDoc{createdPolicy.Statement},
		})
	},
}

//nolint:gochecknoinits
func init() {
	authPoliciesCreate.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesCreate.MarkFlagRequired("id")
	authPoliciesCreate.Flags().String("statement-document", "", "JSON statement document path (or \"-\" for stdin)")
	_ = authPoliciesCreate.MarkFlagRequired("statement-document")

	authPoliciesCmd.AddCommand(authPoliciesCreate)
}
