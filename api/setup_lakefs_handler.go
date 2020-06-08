package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
)

const SetupLakeFSRoute = "/setup_lakefs"

// setupLakeFSHandler setup DB and initial admin user
//   returns 200 (ok) on creation with key/secret - content type json
//   returns 409 (conflict) when user is found
//   return 500 (internal error) if error during operation
func setupLakeFSHandler(authService auth.Service, migrator db.Migrator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		// skip migrate in case we have a user
		if _, err := authService.GetFirstUser(); err == nil {
			w.WriteHeader(http.StatusConflict)
			return
		}

		err := migrator.Migrate(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var req model.User
		err = json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return

		}
		if len(req.DisplayName) == 0 {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		user := &model.User{
			CreatedAt:   time.Now(),
			DisplayName: req.DisplayName,
		}
		cred, err := SetupAdminUser(authService, user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := model.CredentialKeys{
			AccessKeyId:     cred.AccessKeyId,
			AccessSecretKey: cred.AccessSecretKey,
		}
		respJSON, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(respJSON)
	})
}

func createGroups(authService auth.Service, groups []*model.Group) error {
	for _, group := range groups {
		err := authService.CreateGroup(group)
		if err != nil {
			return err
		}
	}
	return nil
}

func createPolicies(authService auth.Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.CreatePolicy(policy)
		if err != nil {
			return err
		}
	}
	return nil
}

func attachPolicies(authService auth.Service, groupId string, policyIds []string) error {
	for _, policyId := range policyIds {
		err := authService.AttachPolicyToGroup(policyId, groupId)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetupAdminUser(authService auth.Service, user *model.User) (*model.Credential, error) {
	now := time.Now()
	var err error

	err = createGroups(authService, []*model.Group{
		{CreatedAt: now, DisplayName: "Admins"},
		{CreatedAt: now, DisplayName: "SuperUsers"},
		{CreatedAt: now, DisplayName: "Developers"},
		{CreatedAt: now, DisplayName: "Viewers"},
	})
	if err != nil {
		return nil, err
	}

	err = createPolicies(authService, []*model.Policy{
		{
			CreatedAt:   now,
			DisplayName: "FSFullAccess",
			Action: []string{
				"fs:*",
			},
			Resource: permissions.All,
			Effect:   true,
		},
		{
			CreatedAt:   now,
			DisplayName: "FSReadAll",
			Action: []string{
				"fs:List*",
				"fs:Read*",
			},
			Resource: permissions.All,
			Effect:   true,
		},
		{
			CreatedAt:   now,
			DisplayName: "FSDenyAdmin",
			Action: []string{
				string(permissions.DeleteRepositoryAction),
				string(permissions.CreateRepositoryAction),
			},
			Resource: permissions.All,
			Effect:   false,
		},
		{
			CreatedAt:   now,
			DisplayName: "AuthFullAccess",
			Action: []string{
				"auth:*",
			},
			Resource: permissions.All,
			Effect:   true,
		},
		{
			CreatedAt:   now,
			DisplayName: "AuthManageOwnCredentials",
			Action: []string{
				string(permissions.CreateCredentialsAction),
				string(permissions.DeleteCredentialsAction),
				string(permissions.ListCredentialsAction),
				string(permissions.ReadCredentialsAction),
			},
			Resource: permissions.UserArn("${user}"),
			Effect:   true,
		},
	})

	err = attachPolicies(authService, "Admins", []string{"FSFullAccess", "AuthFullAccess"})
	if err != nil {
		return nil, err
	}
	err = attachPolicies(authService, "SuperUsers", []string{"FSFullAccess", "AuthManageOwnCredentials"})
	if err != nil {
		return nil, err
	}
	err = attachPolicies(authService, "Developers", []string{"FSFullAccess", "FSDenyAdmin", "AuthManageOwnCredentials"})
	if err != nil {
		return nil, err
	}
	err = attachPolicies(authService, "Viewers", []string{"FSReadAll", "AuthManageOwnCredentials"})
	if err != nil {
		return nil, err
	}

	// create admin user
	err = authService.CreateUser(user)
	if err != nil {
		return nil, err
	}
	err = authService.AddUserToGroup(user.DisplayName, "Admins")
	if err != nil {
		return nil, err
	}
	return authService.CreateCredentials(user.DisplayName)
}
