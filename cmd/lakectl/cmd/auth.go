package cmd

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/api"

	"github.com/spf13/cobra"
)

var userCreatedTemplate = `{{ "User created successfully." | green }}
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate |date }}
`

var groupCreatedTemplate = `{{ "Group created successfully." | green }}
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate |date }}
`

var credentialsCreatedTemplate = `{{ "Credentials created successfully." | green }}
{{ "Access Key ID:" | ljust 18 }} {{ .AccessKeyID | bold }}
{{ "Access Secret Key:" | ljust 18 }} {{  .AccessSecretKey | bold }}

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
	Short: "manage authentication and authorization",
	Long:  "manage authentication and authorization including users, groups and policies",
}

var authUsers = &cobra.Command{
	Use:   "users",
	Short: "manage users",
}

var authUsersList = &cobra.Command{
	Use:   "list",
	Short: "list users",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		res, err := clt.ListUsersWithResponse(cmd.Context(), &api.ListUsersParams{
			After:  &after,
			Amount: &amount,
		})
		if err != nil {
			DieErr(err)
		}

		users := *res.JSON200.Results
		rows := make([][]interface{}, len(users))
		for i, user := range users {
			ts := time.Unix(*user.CreationDate, 0).String()
			rows[i] = []interface{}{user.Id, ts}
		}

		pagination := res.JSON200.Pagination
		PrintTable(rows, []interface{}{"User ID", "Creation Date"}, pagination, amount)
	},
}

var authUsersCreate = &cobra.Command{
	Use:   "create",
	Short: "create a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		res, err := clt.CreateUserWithResponse(cmd.Context(), api.CreateUserJSONRequestBody{
			Id: id,
		})
		if err != nil {
			DieErr(err)
		}
		user := res.JSON201
		Write(userCreatedTemplate, user)
	},
}

var authUsersDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		_, err := clt.DeleteUserWithResponse(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("User deleted successfully\n")
	},
}

var authUsersGroups = &cobra.Command{
	Use:   "groups",
	Short: "manage user groups",
}

var authUsersGroupsList = &cobra.Command{
	Use:   "list",
	Short: "list groups for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		res, err := clt.ListUserGroupsWithResponse(cmd.Context(), id, &api.ListUserGroupsParams{
			After:  &after,
			Amount: &amount,
		})
		if err != nil {
			DieErr(err)
		}

		groups := *res.JSON200.Results
		rows := make([][]interface{}, len(groups))
		for i, group := range groups {
			ts := time.Unix(*group.CreationDate, 0).String()
			rows[i] = []interface{}{group.Id, ts}
		}

		pagination := res.JSON200.Pagination
		PrintTable(rows, []interface{}{"Group ID", "Creation Date"}, pagination, amount)
	},
}

var authUsersPolicies = &cobra.Command{
	Use:   "policies",
	Short: "manage user policies",
}

var authUsersPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "list policies for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		effective, _ := cmd.Flags().GetBool("effective")

		clt := getClient()

		policies, pagination, err := clt.ListUserPolicies(cmd.Context(), id, effective, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, 0)
		for _, policy := range policies {
			for i, statement := range policy.Statement {
				ts := time.Unix(policy.CreationDate, 0).String()
				rows = append(rows, []interface{}{policy.ID, ts, i, statement.Resource, statement.Effect, strings.Join(statement.Action, ", ")})
			}

		}

		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Statement #", "Resource", "Effect", "Actions"}, pagination, amount)
	},
}

var authUsersPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "attach a policy to a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		err := clt.AttachPolicyToUser(cmd.Context(), id, policy)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy attached successfully\n")
	},
}

var authUsersPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "detach a policy from a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		err := clt.DetachPolicyFromUser(cmd.Context(), id, policy)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy detached successfully\n")
	},
}

var authUsersCredentials = &cobra.Command{
	Use:   "credentials",
	Short: "manage user credentials",
}

var authUsersCredentialsCreate = &cobra.Command{
	Use:   "create",
	Short: "create user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		if id == "" {
			user, err := clt.GetCurrentUser(cmd.Context())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		credentials, err := clt.CreateCredentials(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Write(credentialsCreatedTemplate, credentials)
	},
}

var authUsersCredentialsDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		accessKeyID, _ := cmd.Flags().GetString("access-key-id")
		clt := getClient()

		if id == "" {
			user, err := clt.GetCurrentUser(cmd.Context())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		err := clt.DeleteCredentials(cmd.Context(), id, accessKeyID)
		if err != nil {
			DieErr(err)
		}

		Fmt("Credentials deleted successfully\n")
	},
}

var authUsersCredentialsList = &cobra.Command{
	Use:   "list",
	Short: "list user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		id, _ := cmd.Flags().GetString("id")

		clt := getClient()
		if id == "" {
			user, err := clt.GetCurrentUser(cmd.Context())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		credentials, pagination, err := clt.ListUserCredentials(cmd.Context(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(credentials))
		for i, c := range credentials {

			ts := time.Unix(c.CreationDate, 0).String()
			rows[i] = []interface{}{c.AccessKeyID, ts}
		}

		PrintTable(rows, []interface{}{"Access Key ID", "Issued Date"}, pagination, amount)
	},
}

// groups
var authGroups = &cobra.Command{
	Use:   "groups",
	Short: "manage groups",
}

var authGroupsList = &cobra.Command{
	Use:   "list",
	Short: "list groups",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		groups, pagination, err := clt.ListGroups(cmd.Context(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(groups))
		for i, group := range groups {
			ts := time.Unix(group.CreationDate, 0).String()
			rows[i] = []interface{}{group.ID, ts}
		}

		PrintTable(rows, []interface{}{"Group ID", "Creation Date"}, pagination, amount)
	},
}

var authGroupsCreate = &cobra.Command{
	Use:   "create",
	Short: "create a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		user, err := clt.CreateGroup(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Write(groupCreatedTemplate, user)
	},
}

var authGroupsDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		err := clt.DeleteGroup(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("Group deleted successfully\n")
	},
}

var authGroupsMembers = &cobra.Command{
	Use:   "members",
	Short: "manage group user memberships",
}

var authGroupsListMembers = &cobra.Command{
	Use:   "list",
	Short: "list users in a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		users, pagination, err := clt.ListGroupMembers(cmd.Context(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(users))
		for i, user := range users {
			rows[i] = []interface{}{user.ID}
		}

		PrintTable(rows, []interface{}{"User ID"}, pagination, amount)
	},
}

var authGroupsAddMember = &cobra.Command{
	Use:   "add",
	Short: "add a user to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		err := clt.AddGroupMembership(cmd.Context(), id, user)
		if err != nil {
			DieErr(err)
		}

		Fmt("User successfully added\n")
	},
}

var authGroupsRemoveMember = &cobra.Command{
	Use:   "remove",
	Short: "remove a user from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		clt := getClient()

		err := clt.DeleteGroupMembership(cmd.Context(), id, user)
		if err != nil {
			DieErr(err)
		}

		Fmt("User successfully removed\n")
	},
}

var authGroupsPolicies = &cobra.Command{
	Use:   "policies",
	Short: "manage group policies",
}

var authGroupsPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "list policies for the given group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		policies, pagination, err := clt.ListGroupPolicies(cmd.Context(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, 0)
		for _, policy := range policies {
			for i, statement := range policy.Statement {
				ts := time.Unix(policy.CreationDate, 0).String()
				rows = append(rows, []interface{}{policy.ID, ts, i, statement.Resource, statement.Effect, strings.Join(statement.Action, ", ")})
			}

		}

		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Statement #", "Resource", "Effect", "Actions"}, pagination, amount)
	},
}

var authGroupsPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "attach a policy to a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		err := clt.AttachPolicyToGroup(cmd.Context(), id, policy)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy attached successfully\n")
	},
}

var authGroupsPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "detach a policy from a group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		policy, _ := cmd.Flags().GetString("policy")
		clt := getClient()

		err := clt.DetachPolicyFromGroup(cmd.Context(), id, policy)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy detached successfully\n")
	},
}

// policies
var authPolicies = &cobra.Command{
	Use:   "policies",
	Short: "manage policies",
}

var authPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "list policies",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		policies, pagination, err := clt.ListPolicies(cmd.Context(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(policies))
		for i, policy := range policies {
			ts := time.Unix(policy.CreationDate, 0).String()
			rows[i] = []interface{}{*policy.ID, ts}
		}
		PrintTable(rows, []interface{}{"Policy ID", "Creation Date"}, pagination, amount)
	},
}

var authPoliciesCreate = &cobra.Command{
	Use:   "create",
	Short: "create a policy",
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
		}

		var doc StatementDoc
		err = json.NewDecoder(fp).Decode(&doc)
		if err != nil {
			DieFmt("could not parse statement JSON document: %v", err)
		}

		createdPolicy, err := clt.CreatePolicy(cmd.Context(), &models.Policy{ID: &id, Statement: doc.Statement})
		if err != nil {
			DieErr(err)
		}

		Write(policyCreatedTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc StatementDoc
		}{
			ID:           *createdPolicy.ID,
			CreationDate: createdPolicy.CreationDate,
			StatementDoc: StatementDoc{createdPolicy.Statement},
		})
	},
}

type StatementDoc struct {
	Statement []*models.Statement `json:"statement"`
}

var authPoliciesShow = &cobra.Command{
	Use:   "show",
	Short: "show a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		policy, err := clt.GetPolicy(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Write(policyDetailsTemplate, struct {
			ID           string
			CreationDate int64
			StatementDoc StatementDoc
		}{
			ID:           *policy.ID,
			CreationDate: policy.CreationDate,
			StatementDoc: StatementDoc{policy.Statement},
		})
	},
}

var authPoliciesDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		err := clt.DeletePolicy(cmd.Context(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy deleted successfully\n")
	},
}

func addPaginationFlags(cmd *cobra.Command) {
	cmd.Flags().Int("amount", 100, "how many results to return")
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
