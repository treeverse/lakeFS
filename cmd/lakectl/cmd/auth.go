package cmd

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

var userCreatedTemplate = `{{ "User created successfully." | green }}
ID: {{ .Id | bold }}
Creation Date: {{  .CreationDate |date }}
`

var groupCreatedTemplate = `{{ "Group created successfully." | green }}
ID: {{ .Id | bold }}
Creation Date: {{  .CreationDate |date }}
`

var credentialsCreatedTemplate = `{{ "Credentials created successfully." | green }}
{{ "Access Key ID:" | ljust 18 }} {{ .AccessKeyId | bold }}
{{ "Secret Access Key:" | ljust 18 }} {{  .SecretAccessKey | bold }}

{{ "Keep these somewhere safe since you will not be able to see the secret key again" | yellow }}
`

var policyDetailsTemplate = `
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate | date }}
Statements:
{{ .StatementDoc | json }}

`

var policyCreatedTemplate = `{{ "Policy created successfully." | green }}
` + policyDetailsTemplate

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "Manage authentication and authorization",
	Long:  "manage authentication and authorization including users, groups and policies",
}

var authUsers = &cobra.Command{
	Use:   "users",
	Short: "Manage users",
}

var authUsersList = &cobra.Command{
	Use:   "list",
	Short: "List users",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListUsersWithResponse(cmd.Context(), &api.ListUsersParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		users := resp.JSON200.Results
		rows := make([][]interface{}, len(users))
		for i, user := range users {
			ts := time.Unix(user.CreationDate, 0).String()
			rows[i] = []interface{}{user.Id, ts}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"User ID", "Creation Date"}, &pagination, amount)
	},
}

var authUsersCreate = &cobra.Command{
	Use:   "create",
	Short: "Create a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.CreateUserWithResponse(cmd.Context(), api.CreateUserJSONRequestBody{
			Id: id,
		})
		DieOnResponseError(resp, err)
		user := resp.JSON201
		Write(userCreatedTemplate, user)
	},
}

var authUsersDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.DeleteUserWithResponse(cmd.Context(), id)
		DieOnResponseError(resp, err)
		Fmt("User deleted successfully\n")
	},
}

var authUsersGroups = &cobra.Command{
	Use:   "groups",
	Short: "Manage user groups",
}

var authUsersGroupsList = &cobra.Command{
	Use:   "list",
	Short: "List groups for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListUserGroupsWithResponse(cmd.Context(), id, &api.ListUserGroupsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		groups := resp.JSON200.Results
		rows := make([][]interface{}, len(groups))
		for i, group := range groups {
			ts := time.Unix(group.CreationDate, 0).String()
			rows[i] = []interface{}{group.Id, ts}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Group ID", "Creation Date"}, &pagination, amount)
	},
}

var authUsersPolicies = &cobra.Command{
	Use:   "policies",
	Short: "Manage user policies",
}

var authUsersPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		effective, _ := cmd.Flags().GetBool("effective")

		clt := getClient()

		resp, err := clt.ListUserPoliciesWithResponse(cmd.Context(), id, &api.ListUserPoliciesParams{
			After:     api.PaginationAfterPtr(after),
			Amount:    api.PaginationAmountPtr(amount),
			Effective: &effective,
		})
		DieOnResponseError(resp, err)

		policies := resp.JSON200.Results
		rows := make([][]interface{}, 0)
		for _, policy := range policies {
			for i, statement := range policy.Statement {
				ts := time.Unix(*policy.CreationDate, 0).String()
				rows = append(rows, []interface{}{policy.Id, ts, i, statement.Resource, statement.Effect, strings.Join(statement.Action, ", ")})
			}

		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Statement #", "Resource", "Effect", "Actions"}, &pagination, amount)
	},
}

var authUsersPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "Attach a policy to a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()
		resp, err := clt.AttachPolicyToUserWithResponse(cmd.Context(), id, policy)
		DieOnResponseError(resp, err)
		Fmt("Policy attached successfully\n")
	},
}

var authUsersPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "Detach a policy from a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		resp, err := clt.DetachPolicyFromUserWithResponse(cmd.Context(), id, policy)
		DieOnResponseError(resp, err)

		Fmt("Policy detached successfully\n")
	},
}

var authUsersCredentials = &cobra.Command{
	Use:   "credentials",
	Short: "Manage user credentials",
}

var authUsersCredentialsCreate = &cobra.Command{
	Use:   "create",
	Short: "Create user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnResponseError(resp, err)
			id = resp.JSON200.User.Id
		}

		resp, err := clt.CreateCredentialsWithResponse(cmd.Context(), id)
		DieOnResponseError(resp, err)

		credentials := resp.JSON201
		Write(credentialsCreatedTemplate, credentials)
	},
}

var authUsersCredentialsDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		accessKeyID, _ := cmd.Flags().GetString("access-key-id")
		clt := getClient()

		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnResponseError(resp, err)
			id = resp.JSON200.User.Id
		}
		resp, err := clt.DeleteCredentialsWithResponse(cmd.Context(), id, accessKeyID)
		DieOnResponseError(resp, err)

		Fmt("Credentials deleted successfully\n")
	},
}

var authUsersCredentialsList = &cobra.Command{
	Use:   "list",
	Short: "List user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		id, _ := cmd.Flags().GetString("id")

		clt := getClient()
		if id == "" {
			resp, err := clt.GetCurrentUserWithResponse(cmd.Context())
			DieOnResponseError(resp, err)
			id = resp.JSON200.User.Id
		}

		resp, err := clt.ListUserCredentialsWithResponse(cmd.Context(), id, &api.ListUserCredentialsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		credentials := resp.JSON200.Results
		rows := make([][]interface{}, len(credentials))
		for i, c := range credentials {
			ts := time.Unix(c.CreationDate, 0).String()
			rows[i] = []interface{}{c.AccessKeyId, ts}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Access Key ID", "Issued Date"}, &pagination, amount)
	},
}

// groups
var authGroups = &cobra.Command{
	Use:   "groups",
	Short: "Manage groups",
}

var authGroupsList = &cobra.Command{
	Use:   "list",
	Short: "List groups",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListGroupsWithResponse(cmd.Context(), &api.ListGroupsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		groups := resp.JSON200.Results
		rows := make([][]interface{}, len(groups))
		for i, group := range groups {
			ts := time.Unix(group.CreationDate, 0).String()
			rows[i] = []interface{}{group.Id, ts}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Group ID", "Creation Date"}, &pagination, amount)
	},
}

var authGroupsCreate = &cobra.Command{
	Use:   "create",
	Short: "Create a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.CreateGroupWithResponse(cmd.Context(), api.CreateGroupJSONRequestBody{
			Id: id,
		})
		DieOnResponseError(resp, err)
		group := resp.JSON201
		Write(groupCreatedTemplate, group)
	},
}

var authGroupsDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.DeleteGroupWithResponse(cmd.Context(), id)
		DieOnResponseError(resp, err)
		Fmt("Group deleted successfully\n")
	},
}

var authGroupsMembers = &cobra.Command{
	Use:   "members",
	Short: "Manage group user memberships",
}

var authGroupsListMembers = &cobra.Command{
	Use:   "list",
	Short: "List users in a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListGroupMembersWithResponse(cmd.Context(), id, &api.ListGroupMembersParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		users := resp.JSON200.Results
		rows := make([][]interface{}, len(users))
		for i, user := range users {
			rows[i] = []interface{}{user.Id}
		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"User ID"}, &pagination, amount)
	},
}

var authGroupsAddMember = &cobra.Command{
	Use:   "add",
	Short: "Add a user to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		resp, err := clt.AddGroupMembershipWithResponse(cmd.Context(), id, user)
		DieOnResponseError(resp, err)
		Fmt("User successfully added\n")
	},
}

var authGroupsRemoveMember = &cobra.Command{
	Use:   "remove",
	Short: "Remove a user from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		resp, err := clt.DeleteGroupMembershipWithResponse(cmd.Context(), id, user)
		DieOnResponseError(resp, err)
		Fmt("User successfully removed\n")
	},
}

var authGroupsPolicies = &cobra.Command{
	Use:   "policies",
	Short: "Manage group policies",
}

var authGroupsPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies for the given group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListGroupPoliciesWithResponse(cmd.Context(), id, &api.ListGroupPoliciesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		policies := resp.JSON200.Results
		rows := make([][]interface{}, 0)
		for _, policy := range policies {
			for i, statement := range policy.Statement {
				ts := time.Unix(*policy.CreationDate, 0).String()
				rows = append(rows, []interface{}{policy.Id, ts, i, statement.Resource, statement.Effect, strings.Join(statement.Action, ", ")})
			}

		}

		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Statement #", "Resource", "Effect", "Actions"}, &pagination, amount)
	},
}

var authGroupsPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "Attach a policy to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		resp, err := clt.AttachPolicyToGroupWithResponse(cmd.Context(), id, policy)
		DieOnResponseError(resp, err)

		Fmt("Policy attached successfully\n")
	},
}

var authGroupsPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "Detach a policy from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		resp, err := clt.DetachPolicyFromGroupWithResponse(cmd.Context(), id, policy)
		DieOnResponseError(resp, err)

		Fmt("Policy detached successfully\n")
	},
}

// policies
var authPolicies = &cobra.Command{
	Use:   "policies",
	Short: "Manage policies",
}

var authPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "List policies",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		resp, err := clt.ListPoliciesWithResponse(cmd.Context(), &api.ListPoliciesParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnResponseError(resp, err)

		policies := resp.JSON200.Results
		rows := make([][]interface{}, len(policies))
		for i, policy := range policies {
			ts := time.Unix(*policy.CreationDate, 0).String()
			rows[i] = []interface{}{policy.Id, ts}
		}
		pagination := resp.JSON200.Pagination
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date"}, &pagination, amount)
	},
}

var authPoliciesCreate = &cobra.Command{
	Use:   "create",
	Short: "Create a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		document, _ := cmd.Flags().GetString("statement-document")
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
		resp, err := clt.CreatePolicyWithResponse(cmd.Context(), api.CreatePolicyJSONRequestBody{
			Id:        id,
			Statement: doc.Statement,
		})
		DieOnResponseError(resp, err)

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

type StatementDoc struct {
	Statement []api.Statement `json:"statement"`
}

var authPoliciesShow = &cobra.Command{
	Use:   "show",
	Short: "Show a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.GetPolicyWithResponse(cmd.Context(), id)
		DieOnResponseError(resp, err)

		policy := *resp.JSON200
		Write(policyDetailsTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc StatementDoc
		}{
			ID:           policy.Id,
			CreationDate: *policy.CreationDate,
			StatementDoc: StatementDoc{Statement: policy.Statement},
		})
	},
}

var authPoliciesDelete = &cobra.Command{
	Use:   "delete",
	Short: "Delete a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.DeletePolicyWithResponse(cmd.Context(), id)
		DieOnResponseError(resp, err)
		Fmt("Policy deleted successfully\n")
	},
}

func addPaginationFlags(cmd *cobra.Command) {
	cmd.Flags().Int("amount", defaultAmountArgumentValue, "how many results to return")
	cmd.Flags().String("after", "", "show results after this value (used for pagination)")
}

//nolint:gochecknoinits
func init() {
	// users
	authUsersCreate.Flags().String("id", "", "user identifier")
	_ = authUsersCreate.MarkFlagRequired("id")

	authUsersDelete.Flags().String("id", "", "user identifier")
	_ = authUsersDelete.MarkFlagRequired("id")

	addPaginationFlags(authUsersList)

	addPaginationFlags(authUsersGroupsList)
	authUsersGroupsList.Flags().String("id", "", "user identifier")
	_ = authUsersGroupsList.MarkFlagRequired("id")

	authUsersGroups.AddCommand(authUsersGroupsList)

	addPaginationFlags(authUsersPoliciesList)
	authUsersPoliciesList.Flags().Bool("effective", false,
		"list all distinct policies attached to the user, even through group memberships")
	authUsersPoliciesList.Flags().String("id", "", "user identifier")
	_ = authUsersPoliciesList.MarkFlagRequired("id")

	authUsersPoliciesAttach.Flags().String("id", "", "user identifier")
	_ = authUsersPoliciesAttach.MarkFlagRequired("id")
	authUsersPoliciesAttach.Flags().String("policy", "", "policy identifier")
	_ = authUsersPoliciesAttach.MarkFlagRequired("policy")

	authUsersPoliciesDetach.Flags().String("id", "", "user identifier")
	_ = authUsersPoliciesDetach.MarkFlagRequired("id")
	authUsersPoliciesDetach.Flags().String("policy", "", "policy identifier")
	_ = authUsersPoliciesDetach.MarkFlagRequired("policy")

	authUsersPolicies.AddCommand(authUsersPoliciesList)
	authUsersPolicies.AddCommand(authUsersPoliciesAttach)
	authUsersPolicies.AddCommand(authUsersPoliciesDetach)

	authUsersCredentialsList.Flags().String("id", "", "user identifier (default: current user)")
	addPaginationFlags(authUsersCredentialsList)

	authUsersCredentialsCreate.Flags().String("id", "", "user identifier (default: current user)")

	authUsersCredentialsDelete.Flags().String("id", "", "user identifier (default: current user)")
	authUsersCredentialsDelete.Flags().String("access-key-id", "", "access key ID to delete")
	_ = authUsersCredentialsDelete.MarkFlagRequired("access-key-id")

	authUsersCredentials.AddCommand(authUsersCredentialsList)
	authUsersCredentials.AddCommand(authUsersCredentialsCreate)
	authUsersCredentials.AddCommand(authUsersCredentialsDelete)

	authUsers.AddCommand(authUsersCreate)
	authUsers.AddCommand(authUsersDelete)
	authUsers.AddCommand(authUsersList)
	authUsers.AddCommand(authUsersPolicies)
	authUsers.AddCommand(authUsersGroups)
	authUsers.AddCommand(authUsersCredentials)

	authCmd.AddCommand(authUsers)

	// groups
	authGroupsCreate.Flags().String("id", "", "group identifier")
	_ = authGroupsCreate.MarkFlagRequired("id")

	authGroupsDelete.Flags().String("id", "", "group identifier")
	_ = authGroupsDelete.MarkFlagRequired("id")

	addPaginationFlags(authGroupsList)

	authGroupsAddMember.Flags().String("id", "", "group identifier")
	authGroupsAddMember.Flags().String("user", "", "user identifier to add to the group")
	_ = authGroupsAddMember.MarkFlagRequired("id")
	_ = authGroupsAddMember.MarkFlagRequired("user")
	authGroupsRemoveMember.Flags().String("id", "", "group identifier")
	authGroupsRemoveMember.Flags().String("user", "", "user identifier to add to the group")
	_ = authGroupsRemoveMember.MarkFlagRequired("id")
	_ = authGroupsRemoveMember.MarkFlagRequired("user")
	authGroupsListMembers.Flags().String("id", "", "group identifier")
	addPaginationFlags(authGroupsListMembers)
	_ = authGroupsListMembers.MarkFlagRequired("id")

	authGroupsMembers.AddCommand(authGroupsAddMember)
	authGroupsMembers.AddCommand(authGroupsRemoveMember)
	authGroupsMembers.AddCommand(authGroupsListMembers)

	addPaginationFlags(authGroupsPoliciesList)
	authGroupsPoliciesList.Flags().String("id", "", "group identifier")
	_ = authGroupsPoliciesList.MarkFlagRequired("id")

	authGroupsPoliciesAttach.Flags().String("id", "", "user identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("id")
	authGroupsPoliciesAttach.Flags().String("policy", "", "policy identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("policy")

	authGroupsPoliciesDetach.Flags().String("id", "", "user identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("id")
	authGroupsPoliciesDetach.Flags().String("policy", "", "policy identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("policy")

	authGroupsPolicies.AddCommand(authGroupsPoliciesList)
	authGroupsPolicies.AddCommand(authGroupsPoliciesAttach)
	authGroupsPolicies.AddCommand(authGroupsPoliciesDetach)

	authGroups.AddCommand(authGroupsDelete)
	authGroups.AddCommand(authGroupsCreate)
	authGroups.AddCommand(authGroupsList)
	authGroups.AddCommand(authGroupsMembers)
	authGroups.AddCommand(authGroupsPolicies)
	authCmd.AddCommand(authGroups)

	// policies
	authPoliciesCreate.Flags().String("id", "", "policy identifier")
	_ = authPoliciesCreate.MarkFlagRequired("id")
	authPoliciesCreate.Flags().String("statement-document", "", "JSON statement document path (or \"-\" for stdin)")
	_ = authPoliciesCreate.MarkFlagRequired("statement-document")

	authPoliciesDelete.Flags().String("id", "", "policy identifier")
	_ = authPoliciesDelete.MarkFlagRequired("id")

	authPoliciesShow.Flags().String("id", "", "policy identifier")
	_ = authPoliciesShow.MarkFlagRequired("id")

	addPaginationFlags(authPoliciesList)

	authPolicies.AddCommand(authPoliciesDelete)
	authPolicies.AddCommand(authPoliciesCreate)
	authPolicies.AddCommand(authPoliciesShow)
	authPolicies.AddCommand(authPoliciesList)
	authCmd.AddCommand(authPolicies)

	// main auth cmd
	rootCmd.AddCommand(authCmd)
}
