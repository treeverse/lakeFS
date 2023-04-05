package cmd

import (
	"encoding/json"
	"io"
	"net/http"
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

var permissionTemplate = "Group {{ .Group }}:{{ .Permission }}\n"

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "Manage authentication and authorization",
	Long:  "manage authentication and authorization including users, groups and ACLs",
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
	Long:  "Manage user policies.  Requires an external authorization server with matching support.",
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

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
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.CreateCredentialsWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}

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
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}
		resp, err := clt.DeleteCredentialsWithResponse(cmd.Context(), id, accessKeyID)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

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
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			id = resp.JSON200.User.Id
		}

		resp, err := clt.ListUserCredentialsWithResponse(cmd.Context(), id, &api.ListUserCredentialsParams{
			After:  api.PaginationAfterPtr(after),
			Amount: api.PaginationAmountPtr(amount),
		})
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		Fmt("User successfully removed\n")
	},
}

var authGroupsPolicies = &cobra.Command{
	Use:   "policies",
	Short: "Manage group policies",
	Long:  "Manage group policies.  Requires an external authorization server with matching support.",
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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}

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
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
		Fmt("Policy deleted successfully\n")
	},
}

var aclGroupCmd = &cobra.Command{
	Use:   "acl [sub-command]",
	Short: "Manage ACLs",
	Long:  "manage ACLs of groups",
}

var aclGroupACLGet = &cobra.Command{
	Use:   "get",
	Short: "Get ACL of group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		resp, err := clt.GetGroupACLWithResponse(cmd.Context(), id)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		Write(permissionTemplate, struct{ Group, Permission string }{id, resp.JSON200.Permission})
		repositories := resp.JSON200.Repositories
		if *resp.JSON200.AllRepositories {
			Write("{{\"All repositories\" | red | bold}}\n", nil)
		} else {
			rows := make([][]interface{}, len(*repositories))
			for i, r := range *repositories {
				rows[i] = []interface{}{r}
			}
			pagination := api.Pagination{
				HasMore:    false,
				MaxPerPage: len(rows),
				Results:    len(rows),
			}
			PrintTable(rows, []interface{}{"Repository"}, &pagination, len(rows))
		}
	},
}

var aclGroupACLSet = &cobra.Command{
	Use:   "set",
	Short: "Set ACL of group",
	Long:  `Set ACL of group.  permission will be attached to all-repositories or to specified repositories.  You must specify exactly one of --all-repositories or --repositories.`,
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		permission, _ := cmd.Flags().GetString("permission")
		useAllRepositories, _ := cmd.Flags().GetBool("all-repositories")
		useRepositories, _ := cmd.Flags().GetStringSlice("repositories")

		if !useAllRepositories && len(useRepositories) == 0 {
			DieFmt("Must specify exactly one of --all-repositories or --repositories")
		}

		clt := getClient()

		repositories := make([]string, 0, len(useRepositories))
		if !useAllRepositories {
			repositories = useRepositories
		}

		acl := api.SetGroupACLJSONRequestBody{
			Permission:      permission,
			AllRepositories: &useAllRepositories,
			Repositories:    &repositories,
		}

		resp, err := clt.SetGroupACL(cmd.Context(), id, acl)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
	},
}

func addPaginationFlags(cmd *cobra.Command) {
	cmd.Flags().SortFlags = false
	cmd.Flags().Int("amount", defaultAmountArgumentValue, "how many results to return")
	cmd.Flags().String("after", "", "show results after this value (used for pagination)")
}

//nolint:gochecknoinits
func init() {
	// users
	authUsersCreate.Flags().String("id", "", "Username")
	_ = authUsersCreate.MarkFlagRequired("id")

	authUsersDelete.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersDelete.MarkFlagRequired("id")

	authUsersGroupsList.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersGroupsList.MarkFlagRequired("id")

	authUsersGroups.AddCommand(authUsersGroupsList)

	authUsersPoliciesList.Flags().Bool("effective", false,
		"List all distinct policies attached to the user, including by group memberships")
	authUsersPoliciesList.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesList.MarkFlagRequired("id")

	authUsersPoliciesAttach.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesAttach.MarkFlagRequired("id")
	authUsersPoliciesAttach.Flags().String("policy", "", "Policy identifier")
	_ = authUsersPoliciesAttach.MarkFlagRequired("policy")

	authUsersPoliciesDetach.Flags().String("id", "", "Username (email for password-based users)")
	_ = authUsersPoliciesDetach.MarkFlagRequired("id")
	authUsersPoliciesDetach.Flags().String("policy", "", "Policy identifier")
	_ = authUsersPoliciesDetach.MarkFlagRequired("policy")

	authUsersPolicies.AddCommand(authUsersPoliciesList)
	authUsersPolicies.AddCommand(authUsersPoliciesAttach)
	authUsersPolicies.AddCommand(authUsersPoliciesDetach)

	authUsersCredentialsList.Flags().String("id", "", "Username (email for password-based users, default: current user)")

	authUsersCredentialsCreate.Flags().String("id", "", "Username (email for password-based users, default: current user)")

	authUsersCredentialsDelete.Flags().String("id", "", "Username (email for password-based users, default: current user)")
	authUsersCredentialsDelete.Flags().String("access-key-id", "", "Access key ID to delete")
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
	authGroupsCreate.Flags().String("id", "", "Group identifier")
	_ = authGroupsCreate.MarkFlagRequired("id")

	authGroupsDelete.Flags().String("id", "", "Group identifier")
	_ = authGroupsDelete.MarkFlagRequired("id")

	authGroupsAddMember.Flags().String("id", "", "Group identifier")
	authGroupsAddMember.Flags().String("user", "", "Username (email for password-based users, default: current user)")
	_ = authGroupsAddMember.MarkFlagRequired("id")
	_ = authGroupsAddMember.MarkFlagRequired("user")
	authGroupsRemoveMember.Flags().String("id", "", "Group identifier")
	authGroupsRemoveMember.Flags().String("user", "", "Username (email for password-based users, default: current user)")
	_ = authGroupsRemoveMember.MarkFlagRequired("id")
	_ = authGroupsRemoveMember.MarkFlagRequired("user")
	authGroupsListMembers.Flags().String("id", "", "Group identifier")
	_ = authGroupsListMembers.MarkFlagRequired("id")

	authGroupsMembers.AddCommand(authGroupsAddMember)
	authGroupsMembers.AddCommand(authGroupsRemoveMember)
	authGroupsMembers.AddCommand(authGroupsListMembers)

	authGroupsPoliciesList.Flags().String("id", "", "Group identifier")
	_ = authGroupsPoliciesList.MarkFlagRequired("id")

	authGroupsPoliciesAttach.Flags().String("id", "", "User identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("id")
	authGroupsPoliciesAttach.Flags().String("policy", "", "Policy identifier")
	_ = authGroupsPoliciesAttach.MarkFlagRequired("policy")

	authGroupsPoliciesDetach.Flags().String("id", "", "User identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("id")
	authGroupsPoliciesDetach.Flags().String("policy", "", "Policy identifier")
	_ = authGroupsPoliciesDetach.MarkFlagRequired("policy")

	authGroupsPolicies.AddCommand(authGroupsPoliciesList)
	authGroupsPolicies.AddCommand(authGroupsPoliciesAttach)
	authGroupsPolicies.AddCommand(authGroupsPoliciesDetach)

	aclGroupACLGet.Flags().String("id", "", "Group identifier")
	_ = aclGroupACLGet.MarkFlagRequired("id")

	aclGroupACLSet.Flags().String("id", "", "Group identifier")
	_ = aclGroupACLSet.MarkFlagRequired("id")
	aclGroupACLSet.Flags().String("permission", "", `Permission, typically one of "Reader", "Writer", "Super" or "Admin"`)
	_ = aclGroupACLSet.MarkFlagRequired("permission")
	aclGroupACLSet.Flags().Bool("all-repositories", false, "If set, allow all repositories (current and future)")
	aclGroupACLSet.Flags().StringSlice("repositories", nil, "List of specific repositories to allow for permission")

	aclGroupCmd.AddCommand(aclGroupACLGet)
	aclGroupCmd.AddCommand(aclGroupACLSet)

	authGroups.AddCommand(authGroupsDelete)
	authGroups.AddCommand(authGroupsCreate)
	authGroups.AddCommand(authGroupsList)
	authGroups.AddCommand(authGroupsMembers)
	authGroups.AddCommand(authGroupsPolicies)
	authGroups.AddCommand(aclGroupCmd)
	authCmd.AddCommand(authGroups)

	// policies
	authPoliciesCreate.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesCreate.MarkFlagRequired("id")
	authPoliciesCreate.Flags().String("statement-document", "", "JSON statement document path (or \"-\" for stdin)")
	_ = authPoliciesCreate.MarkFlagRequired("statement-document")

	authPoliciesDelete.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesDelete.MarkFlagRequired("id")

	authPoliciesShow.Flags().String("id", "", "Policy identifier")
	_ = authPoliciesShow.MarkFlagRequired("id")

	authPolicies.AddCommand(authPoliciesDelete)
	authPolicies.AddCommand(authPoliciesCreate)
	authPolicies.AddCommand(authPoliciesShow)
	authPolicies.AddCommand(authPoliciesList)
	authCmd.AddCommand(authPolicies)

	// main auth cmd
	rootCmd.AddCommand(authCmd)
	addPaginationFlags(authUsersList)
	addPaginationFlags(authUsersGroupsList)
	addPaginationFlags(authUsersPoliciesList)
	addPaginationFlags(authUsersCredentialsList)
	addPaginationFlags(authGroupsList)
	addPaginationFlags(authGroupsListMembers)
	addPaginationFlags(authGroupsPoliciesList)
	addPaginationFlags(authPoliciesList)
}
