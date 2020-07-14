package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/treeverse/lakefs/stats"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
)

const SetupLakeFSRoute = "/setup_lakefs"

// setupLakeFSHandler setup DB and initial admin user
//   returns 200 (ok) on creation with key/secret - content type json
//   returns 409 (conflict) when user is found
//   return 500 (internal error) if error during operation
func setupLakeFSHandler(authService auth.Service, meta auth.MetadataManager, migrator db.Migrator, collector stats.Collector) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		// skip migrate in case we have an active installation
		if _, err := meta.InstallationID(); err == nil {
			w.WriteHeader(http.StatusConflict)
			return
		}

		err := migrator.Migrate(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		metadata, err := meta.Write()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		collector.SetInstallationID(metadata["installation_id"])
		collector.CollectMetadata(metadata)
		collector.CollectEvent("global", "init")

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
		cred, err := auth.SetupAdminUser(authService, user)
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
