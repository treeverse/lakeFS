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

func SetupAdminUser(authService auth.Service, user *model.User) (*model.Credential, error) {
	now := time.Now()
	err := authService.CreateUser(user)
	if err != nil {
		return nil, err
	}

	role := &model.Role{
		CreatedAt:   now,
		DisplayName: model.RoleAdmin,
	}

	err = authService.CreateRole(role)
	if err != nil {
		return nil, err
	}

	policies := []*model.Policy{
		{
			CreatedAt:   now,
			DisplayName: "RepoFullAccess",
			Action: []string{
				string(permissions.ManageRepos),
				string(permissions.ReadRepo),
				string(permissions.WriteRepo),
			},
			Resource: permissions.AllReposArn,
			Effect:   true,
		},
		{
			CreatedAt:   now,
			DisplayName: "AuthFullAccess",
			Action: []string{
				string(permissions.ManageRBAC),
			},
			Resource: permissions.RbacArn,
			Effect:   true,
		},
	}

	for _, policy := range policies {
		err = authService.CreatePolicy(policy)
		if err != nil {
			return nil, err
		}
		err = authService.AttachPolicyToRole(role.DisplayName, policy.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	err = authService.AttachRoleToUser(role.DisplayName, user.DisplayName)
	if err != nil {
		return nil, err
	}

	return authService.CreateCredentials(user.DisplayName)
}
