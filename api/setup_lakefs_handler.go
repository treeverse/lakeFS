package api

import (
	"encoding/json"
	"net/http"

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
		if len(req.Email) == 0 || len(req.FullName) == 0 {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		user := &model.User{
			Email:    req.Email,
			FullName: req.FullName,
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
	err := authService.CreateUser(user)
	if err != nil {
		return nil, err
	}

	role := &model.Role{
		DisplayName: model.RoleAdmin,
	}

	err = authService.CreateRole(role)
	if err != nil {
		return nil, err
	}
	policies := []*model.Policy{
		{
			Permission: string(permissions.ManageRepos),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.ReadRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.WriteRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
	}
	for _, policy := range policies {
		err = authService.AssignPolicyToRole(role.Id, policy)
		if err != nil {
			return nil, err
		}
	}

	err = authService.AssignRoleToUser(role.Id, user.Id)
	if err != nil {
		return nil, err
	}

	return authService.CreateUserCredentials(user)
}
