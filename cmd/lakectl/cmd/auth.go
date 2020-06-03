package cmd

import (
	"context"
	"strings"
	"time"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/go-openapi/swag"
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

var roleCreatedTemplate = `{{ "Role created successfully." | green }}
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate |date }}
`

var policyCreatedTemplate = `{{ "Policy created successfully." | green }}
ID: {{ .ID | bold }}
Creation Date: {{  .CreationDate |date }}
Effect: {{ if eq .Effect "Allow" }}{{ .Effect | green }}{{ else }}{{ .Effect | red }}{{ end }}
Resource: {{ .Resource }}
Action: {{ .Action | join ", "}}
`

var credentialsCreatedTemplate = `{{ "Credentials created successfully." | green }}
{{ "Access Key ID:" | ljust 18 }} {{ .AccessKeyID | bold }}
{{ "Access Secret Key:" | ljust 18 }} {{  .AccessSecretKey | bold }}

{{ "Keep these somewhere safe since you will not be able to see the secret key again" | yellow }}
`

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "manage authentication and authorization",
	Long:  "manage authentication and authorization including users, groups, roles and policies",
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

		users, pagination, err := clt.ListUsers(context.Background(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(users))
		for i, user := range users {
			ts := time.Unix(user.CreationDate, 0).String()
			rows[i] = []interface{}{user.ID, ts}
		}

		PrintTable(rows, []interface{}{"User ID", "Creation Date"}, pagination, amount)
	},
}

var authUsersCreate = &cobra.Command{
	Use:   "create",
	Short: "create a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		user, err := clt.CreateUser(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Write(userCreatedTemplate, user)
	},
}

var authUsersDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		err := clt.DeleteUser(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("User deleted successfully\n")
	},
}

var authUsersRoles = &cobra.Command{
	Use:   "roles",
	Short: "manage user roles",
}

var authUsersRolesList = &cobra.Command{
	Use:   "list",
	Short: "list roles for the given user",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		roles, pagination, err := clt.ListUserRoles(context.Background(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(roles))
		for i, role := range roles {
			ts := time.Unix(role.CreationDate, 0).String()
			rows[i] = []interface{}{role.ID, ts}
		}

		PrintTable(rows, []interface{}{"Role ID", "Creation Date"}, pagination, amount)
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

		groups, pagination, err := clt.ListUserGroups(context.Background(), id, after, amount)
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

		groups, pagination, err := clt.ListGroups(context.Background(), after, amount)
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

		user, err := clt.CreateGroup(context.Background(), id)
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

		err := clt.DeleteGroup(context.Background(), id)
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

		users, pagination, err := clt.ListGroupMembers(context.Background(), id, after, amount)
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

		err := clt.AddGroupMembership(context.Background(), id, user)
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

		err := clt.DeleteGroupMembership(context.Background(), id, user)
		if err != nil {
			DieErr(err)
		}

		Fmt("User successfully removed\n")
	},
}

var authGroupsRoles = &cobra.Command{
	Use:   "roles",
	Short: "manage group roles",
}

var authGroupsRolesList = &cobra.Command{
	Use:   "list",
	Short: "list roles for the given group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		roles, pagination, err := clt.ListGroupRoles(context.Background(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(roles))
		for i, role := range roles {
			ts := time.Unix(role.CreationDate, 0).String()
			rows[i] = []interface{}{role.ID, ts}
		}

		PrintTable(rows, []interface{}{"Role ID", "Creation Date"}, pagination, amount)
	},
}

// roles
var authRoles = &cobra.Command{
	Use:   "roles",
	Short: "manage roles",
}

var authRolesList = &cobra.Command{
	Use:   "list",
	Short: "list roles",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		roles, pagination, err := clt.ListRoles(context.Background(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(roles))
		for i, role := range roles {
			ts := time.Unix(role.CreationDate, 0).String()
			rows[i] = []interface{}{role.ID, ts}
		}

		PrintTable(rows, []interface{}{"Role ID", "Creation Date"}, pagination, amount)
	},
}

var authRolesCreate = &cobra.Command{
	Use:   "create",
	Short: "create a role",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		user, err := clt.CreateRole(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Write(roleCreatedTemplate, user)
	},
}

var authRolesDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a role",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		err := clt.DeleteRole(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("Role deleted successfully\n")
	},
}

var authRolesAttach = &cobra.Command{
	Use:   "attach",
	Short: "attach a role to a user or group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		group, _ := cmd.Flags().GetString("group")

		if (user != "" && group != "") || (user == "" && group == "") {
			DieFmt("please pass either user ID or group ID")
		}

		clt := getClient()

		var err error
		if user != "" {
			err = clt.AttachRoleToUser(context.Background(), user, id)
		} else {
			err = clt.AttachRoleToGroup(context.Background(), group, id)
		}

		if err != nil {
			DieErr(err)
		}

		Fmt("Role attached successfully\n")
	},
}

var authRolesDetach = &cobra.Command{
	Use:   "detach",
	Short: "detach a role from a user or group",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		user, _ := cmd.Flags().GetString("user")
		group, _ := cmd.Flags().GetString("group")

		if (user != "" && group != "") || (user == "" && group == "") {
			DieFmt("please pass either user ID or group ID")
		}

		clt := getClient()

		var err error
		if user != "" {
			err = clt.DetachRoleFromUser(context.Background(), user, id)
		} else {
			err = clt.DetachRoleFromGroup(context.Background(), group, id)
		}

		if err != nil {
			DieErr(err)
		}

		Fmt("Role detached successfully\n")
	},
}
var authRolesPolicies = &cobra.Command{
	Use:   "policies",
	Short: "manage policies",
}

var authRolesPoliciesList = &cobra.Command{
	Use:   "list",
	Short: "list policies attached to role",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt := getClient()

		policies, pagination, err := clt.ListRolePolicies(context.Background(), id, after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(policies))
		for i, policy := range policies {

			ts := time.Unix(policy.CreationDate, 0).String()
			rows[i] = []interface{}{policy.ID, ts, policy.Resource, policy.Effect, strings.Join(policy.Action, ", ")}
		}

		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Resource", "Effect", "Actions"}, pagination, amount)
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

		policies, pagination, err := clt.ListPolicies(context.Background(), after, amount)
		if err != nil {
			DieErr(err)
		}

		rows := make([][]interface{}, len(policies))
		for i, policy := range policies {

			ts := time.Unix(policy.CreationDate, 0).String()
			rows[i] = []interface{}{policy.ID, ts, policy.Resource, policy.Effect, strings.Join(policy.Action, ", ")}
		}

		PrintTable(rows, []interface{}{"Policy ID", "Creation Date", "Resource", "Effect", "Actions"}, pagination, amount)
	},
}

var authPoliciesCreate = &cobra.Command{
	Use:   "create",
	Short: "create a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		effect, _ := cmd.Flags().GetString("effect")
		resource, _ := cmd.Flags().GetString("resource")
		action, _ := cmd.Flags().GetStringSlice("action")
		clt := getClient()

		policy, err := clt.CreatePolicy(context.Background(), &models.PolicyCreation{
			Action:   action,
			Effect:   swag.String(effect),
			ID:       swag.String(id),
			Resource: swag.String(resource),
		})
		if err != nil {
			DieErr(err)
		}

		Write(policyCreatedTemplate, policy)
	},
}

var authPoliciesDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete a policy",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		err := clt.DeletePolicy(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy deleted successfully\n")
	},
}

var authPoliciesAttach = &cobra.Command{
	Use:   "attach",
	Short: "attach a policy to a role",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		role, _ := cmd.Flags().GetString("role")
		clt := getClient()

		err := clt.AttachPolicyToRole(context.Background(), id, role)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy attached successfully\n")
	},
}

var authPoliciesDetach = &cobra.Command{
	Use:   "detach",
	Short: "detach a policy from a role",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		role, _ := cmd.Flags().GetString("role")
		clt := getClient()

		err := clt.DetachPolicyFromRole(context.Background(), id, role)
		if err != nil {
			DieErr(err)
		}

		Fmt("Policy detached successfully\n")
	},
}

// credentials
var authCredentials = &cobra.Command{
	Use:   "credentials",
	Short: "manage user credentials",
}

var authCredentialsCreate = &cobra.Command{
	Use:   "create",
	Short: "create user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		clt := getClient()

		if id == "" {
			user, err := clt.GetCurrentUser(context.Background())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		credentials, err := clt.CreateCredentials(context.Background(), id)
		if err != nil {
			DieErr(err)
		}

		Write(credentialsCreatedTemplate, credentials)
	},
}

var authCredentialsDelete = &cobra.Command{
	Use:   "delete",
	Short: "delete user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		id, _ := cmd.Flags().GetString("id")
		accessKeyId, _ := cmd.Flags().GetString("access-key-id")
		clt := getClient()

		if id == "" {
			user, err := clt.GetCurrentUser(context.Background())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		err := clt.DeleteCredentials(context.Background(), id, accessKeyId)
		if err != nil {
			DieErr(err)
		}

		Fmt("Credentials deleted successfully\n")
	},
}

var authCredentialsList = &cobra.Command{
	Use:   "list",
	Short: "list user credentials",
	Run: func(cmd *cobra.Command, args []string) {
		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")
		id, _ := cmd.Flags().GetString("id")

		clt := getClient()
		if id == "" {
			user, err := clt.GetCurrentUser(context.Background())
			if err != nil {
				DieErr(err)
			}
			id = user.ID
		}

		credentials, pagination, err := clt.ListUserCredentials(context.Background(), id, after, amount)
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

func init() {
	// users
	authUsersCreate.Flags().String("id", "", "user identifier")
	_ = authUsersCreate.MarkFlagRequired("id")

	authUsersDelete.Flags().String("id", "", "user identifier")
	_ = authUsersDelete.MarkFlagRequired("id")

	authUsersList.Flags().Int("amount", 100, "how many results to return")
	authUsersList.Flags().String("after", "", "show results after this value (used for pagination)")

	authUsersRolesList.Flags().String("id", "", "user identifier")
	authUsersRolesList.Flags().Int("amount", 100, "how many results to return")
	authUsersRolesList.Flags().String("after", "", "show results after this value (used for pagination)")
	_ = authUsersRolesList.MarkFlagRequired("id")

	authUsersGroupsList.Flags().String("id", "", "user identifier")
	authUsersGroupsList.Flags().Int("amount", 100, "how many results to return")
	authUsersGroupsList.Flags().String("after", "", "show results after this value (used for pagination)")
	_ = authUsersGroupsList.MarkFlagRequired("id")

	authUsersRoles.AddCommand(authUsersRolesList)
	authUsersGroups.AddCommand(authUsersGroupsList)

	authUsers.AddCommand(authUsersDelete)
	authUsers.AddCommand(authUsersCreate)
	authUsers.AddCommand(authUsersList)
	authUsers.AddCommand(authUsersRoles)
	authUsers.AddCommand(authUsersGroups)

	authCmd.AddCommand(authUsers)

	// groups
	authGroupsCreate.Flags().String("id", "", "group identifier")
	_ = authGroupsCreate.MarkFlagRequired("id")

	authGroupsDelete.Flags().String("id", "", "group identifier")
	_ = authGroupsDelete.MarkFlagRequired("id")

	authGroupsList.Flags().Int("amount", 100, "how many results to return")
	authGroupsList.Flags().String("after", "", "show results after this value (used for pagination)")

	authGroupsAddMember.Flags().String("id", "", "group identifier")
	authGroupsAddMember.Flags().String("user", "", "user identifier to add to the group")
	_ = authGroupsAddMember.MarkFlagRequired("id")
	_ = authGroupsAddMember.MarkFlagRequired("user")
	authGroupsRemoveMember.Flags().String("id", "", "group identifier")
	authGroupsRemoveMember.Flags().String("user", "", "user identifier to add to the group")
	_ = authGroupsRemoveMember.MarkFlagRequired("id")
	_ = authGroupsRemoveMember.MarkFlagRequired("user")
	authGroupsListMembers.Flags().String("id", "", "group identifier")
	authGroupsListMembers.Flags().Int("amount", 100, "how many results to return")
	authGroupsListMembers.Flags().String("after", "", "show results after this value (used for pagination)")
	_ = authGroupsListMembers.MarkFlagRequired("id")
	authGroupsRolesList.Flags().String("id", "", "group identifier")
	authGroupsRolesList.Flags().Int("amount", 100, "how many results to return")
	authGroupsRolesList.Flags().String("after", "", "show results after this value (used for pagination)")
	_ = authGroupsRolesList.MarkFlagRequired("id")

	authGroupsMembers.AddCommand(authGroupsAddMember)
	authGroupsMembers.AddCommand(authGroupsRemoveMember)
	authGroupsMembers.AddCommand(authGroupsListMembers)

	authGroupsRoles.AddCommand(authGroupsRolesList)

	authGroups.AddCommand(authGroupsDelete)
	authGroups.AddCommand(authGroupsCreate)
	authGroups.AddCommand(authGroupsList)
	authGroups.AddCommand(authGroupsMembers)
	authGroups.AddCommand(authGroupsRoles)
	authCmd.AddCommand(authGroups)

	// roles
	authRolesCreate.Flags().String("id", "", "role identifier")
	_ = authRolesCreate.MarkFlagRequired("id")

	authRolesDelete.Flags().String("id", "", "role identifier")
	_ = authRolesDelete.MarkFlagRequired("id")

	authRolesList.Flags().Int("amount", 100, "how many results to return")
	authRolesList.Flags().String("after", "", "show results after this value (used for pagination)")

	authRolesAttach.Flags().String("id", "", "role identifier")
	authRolesAttach.Flags().String("group", "", "group ID to attach role to")
	authRolesAttach.Flags().String("user", "", "user ID to attach role to")
	_ = authRolesAttach.MarkFlagRequired("id")
	authRolesDetach.Flags().String("id", "", "role identifier")
	authRolesDetach.Flags().String("group", "", "group ID to attach role to")
	authRolesDetach.Flags().String("user", "", "user ID to attach role to")
	_ = authRolesDetach.MarkFlagRequired("id")

	authRolesPoliciesList.Flags().String("id", "", "role identifier")
	authRolesPoliciesList.Flags().Int("amount", 100, "how many results to return")
	authRolesPoliciesList.Flags().String("after", "", "show results after this value (used for pagination)")

	authRolesPolicies.AddCommand(authRolesPoliciesList)

	authRoles.AddCommand(authRolesDelete)
	authRoles.AddCommand(authRolesCreate)
	authRoles.AddCommand(authRolesList)
	authRoles.AddCommand(authRolesAttach)
	authRoles.AddCommand(authRolesDetach)
	authRoles.AddCommand(authRolesPolicies)
	authCmd.AddCommand(authRoles)

	// policies
	authPoliciesCreate.Flags().String("id", "", "policy identifier")
	authPoliciesCreate.Flags().String("effect", "Allow", "policy effect (Allow/Deny)")
	authPoliciesCreate.Flags().String("resource", "", "resource ARN")
	authPoliciesCreate.Flags().StringSlice("action", []string{}, "actions to attach to the policy")
	_ = authPoliciesCreate.MarkFlagRequired("id")
	_ = authPoliciesCreate.MarkFlagRequired("resource")
	_ = authPoliciesCreate.MarkFlagRequired("action")

	authPoliciesDelete.Flags().String("id", "", "policy identifier")
	_ = authPoliciesDelete.MarkFlagRequired("id")

	authPoliciesList.Flags().Int("amount", 100, "how many results to return")
	authPoliciesList.Flags().String("after", "", "show results after this value (used for pagination)")

	authPoliciesAttach.Flags().String("id", "", "policy identifier")
	authPoliciesAttach.Flags().String("role", "", "role identifier to attach to")
	_ = authPoliciesAttach.MarkFlagRequired("id")
	authPoliciesDetach.Flags().String("id", "", "policy identifier")
	authPoliciesDetach.Flags().String("role", "", "role identifier to detach from")
	_ = authPoliciesDetach.MarkFlagRequired("id")

	authPolicies.AddCommand(authPoliciesDelete)
	authPolicies.AddCommand(authPoliciesCreate)
	authPolicies.AddCommand(authPoliciesList)
	authPolicies.AddCommand(authPoliciesAttach)
	authPolicies.AddCommand(authPoliciesDetach)
	authCmd.AddCommand(authPolicies)

	// credentials
	authCredentialsCreate.Flags().String("id", "", "user ID to create credentials for (default: current user)")
	authCredentialsDelete.Flags().String("id", "", "user ID to list credentials for (default: current user)")
	authCredentialsDelete.Flags().String("access-key-id", "", "access key ID to be deleted")
	_ = authCredentialsDelete.MarkFlagRequired("access-key-id")
	authCredentialsList.Flags().String("id", "", "user ID to list credentials for (default: current user)")
	authCredentialsList.Flags().Int("amount", 100, "how many results to return")
	authCredentialsList.Flags().String("after", "", "show results after this value (used for pagination)")

	authCredentials.AddCommand(authCredentialsCreate)
	authCredentials.AddCommand(authCredentialsDelete)
	authCredentials.AddCommand(authCredentialsList)
	authCmd.AddCommand(authCredentials)

	// main auth cmd
	rootCmd.AddCommand(authCmd)
}
