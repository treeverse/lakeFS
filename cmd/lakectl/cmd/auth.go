package cmd

import (
	"net/http"
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
		if repositories == nil || len(*repositories) == 0 {
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
	Short: "Set ACL of group id",
	Long:  `Set ACL of group id.  permission will be attached to all-repositories or to specified repositories.  You must specify exactly one of --all-repositories or --repositories.`,
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
			Permission:   permission,
			Repositories: &repositories,
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
	authGroups.AddCommand(aclGroupCmd)

	authCmd.AddCommand(authGroups)

	// main auth cmd
	rootCmd.AddCommand(authCmd)
	addPaginationFlags(authUsersList)
	addPaginationFlags(authUsersGroupsList)
	addPaginationFlags(authUsersCredentialsList)
	addPaginationFlags(authGroupsList)
	addPaginationFlags(authGroupsListMembers)
}
