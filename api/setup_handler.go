package api

import (
	"encoding/json"
	"net/http"

	"github.com/treeverse/lakefs/auth"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
)

// setupHandler setup DB and initial admin user
//   returns 200 (ok) on creation with key/secret - content type json
//   returns 409 (conflict) when user is found
//   return 500 (internal error) if error during operation
func setupHandler(authService auth.Service, migrator db.Migrator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		if _, err := authService.GetUser(1); err == nil {
			// we have a user - skip migrate
			w.WriteHeader(http.StatusConflict)
			return
		}

		err := migrator.Migrate()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		req := struct {
			Email    string `json:"email"`
			FullName string `json:"full_name"`
		}{}
		err = json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return

		}
		if len(req.Email) == 0 || len(req.FullName) == 0 {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		user := &authmodel.User{
			Email:    req.Email,
			FullName: req.FullName,
		}
		cred, err := SetupAdminUser(authService, user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := struct {
			AccessKeyID     string `json:"access_key_id"`
			SecretAccessKey string `json:"secret_access_key"`
		}{
			AccessKeyID:     cred.AccessKeyId,
			SecretAccessKey: cred.AccessSecretKey,
		}

		respJSON, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(respJSON)
	}
}

func SetupAdminUser(authService auth.Service, user *authmodel.User) (*authmodel.Credential, error) {
	err := authService.CreateUser(user)
	if err != nil {
		return nil, err
	}

	role := &authmodel.Role{
		DisplayName: authmodel.RoleAdmin,
	}

	err = authService.CreateRole(role)
	if err != nil {
		return nil, err
	}
	policies := []*authmodel.Policy{
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
