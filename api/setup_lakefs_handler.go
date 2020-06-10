package api

import (
	"encoding/json"
	"fmt"
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
	var err error
	// create groups
	adminsGroup := &model.Group{
		CreatedAt:   now,
		DisplayName: "Admins",
	}
	superUsersGroup := &model.Group{
		CreatedAt:   now,
		DisplayName: "SuperUsers",
	}
	developersGroup := &model.Group{
		CreatedAt:   now,
		DisplayName: "Developers",
	}
	viewersGroup := &model.Group{
		CreatedAt:   now,
		DisplayName: "Viewers",
	}
	for _, group := range []*model.Group{adminsGroup, superUsersGroup, developersGroup, viewersGroup} {
		err = authService.CreateGroup(group)
		if err != nil {
			return nil, err
		}
	}

	repoFullAccessPolicy := &model.Policy{
		CreatedAt:   now,
		DisplayName: "RepoFullAccess",
		Action: []string{
			string(permissions.ManageReposAction),
			string(permissions.ReadRepoAction),
			string(permissions.WriteRepoAction),
		},
		Resource: permissions.AllReposArn,
		Effect:   true,
	}
	repoReaderWriterPolicy := &model.Policy{
		CreatedAt:   now,
		DisplayName: "RepoReadWrite",
		Action: []string{
			string(permissions.ReadRepoAction),
			string(permissions.WriteRepoAction),
		},
		Resource: permissions.AllReposArn,
		Effect:   true,
	}

	authFullAccessPolicy := &model.Policy{
		CreatedAt:   now,
		DisplayName: "AuthFullAccess",
		Action: []string{
			string(permissions.WriteAuthAction),
			string(permissions.ReadAuthAction),
		},
		Resource: permissions.AllAuthArn,
		Effect:   true,
	}
	readReposPolicy := &model.Policy{
		CreatedAt:   now,
		DisplayName: "RepoRead",
		Action: []string{
			string(permissions.ReadRepoAction),
		},
		Resource: permissions.AllReposArn,
		Effect:   true,
	}
	credentialsManagePolicy := &model.Policy{
		CreatedAt:   now,
		DisplayName: "ManageOwnCredentials",
		Action: []string{
			string(permissions.WriteAuthAction),
			string(permissions.ReadAuthAction),
		},
		Resource: fmt.Sprintf(permissions.AuthUserCredentialsArn, "${user}"),
		Effect:   true,
	}

	for _, policy := range []*model.Policy{repoFullAccessPolicy, authFullAccessPolicy, readReposPolicy, repoReaderWriterPolicy, credentialsManagePolicy} {
		err = authService.CreatePolicy(policy)
		if err != nil {
			return nil, err
		}
	}
	for _, policy := range []*model.Policy{repoFullAccessPolicy, authFullAccessPolicy} {
		err = authService.AttachPolicyToGroup(policy.DisplayName, adminsGroup.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	for _, policy := range []*model.Policy{repoFullAccessPolicy} {
		err = authService.AttachPolicyToGroup(policy.DisplayName, superUsersGroup.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	for _, policy := range []*model.Policy{repoReaderWriterPolicy} {
		err = authService.AttachPolicyToGroup(policy.DisplayName, developersGroup.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	for _, policy := range []*model.Policy{readReposPolicy} {
		err = authService.AttachPolicyToGroup(policy.DisplayName, viewersGroup.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	// all groups get credentialsManagerPolicy
	for _, group := range []*model.Group{viewersGroup, developersGroup, superUsersGroup} {
		err = authService.AttachPolicyToGroup(credentialsManagePolicy.DisplayName, group.DisplayName)
		if err != nil {
			return nil, err
		}
	}

	// create admin user
	err = authService.CreateUser(user)
	if err != nil {
		return nil, err
	}
	err = authService.AddUserToGroup(user.DisplayName, adminsGroup.DisplayName)
	if err != nil {
		return nil, err
	}
	return authService.CreateCredentials(user.DisplayName)
}
