package cmd_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	authfactory "github.com/treeverse/lakefs/pkg/auth/factory"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestGetAuthService(t *testing.T) {
	t.Parallel()
	t.Run("maintain_inviter", func(t *testing.T) {
		cfg := &config.ConfigImpl{}
		cfg.Auth.GetAuthUIConfig().RBAC = config.AuthRBACInternal
		cfg.Auth.GetBaseAuthConfig().API.Endpoint = "http://localhost:8000"
		cfg.Auth.GetBaseAuthConfig().API.SkipHealthCheck = true
		service := authfactory.NewAuthService(t.Context(), cfg, logging.ContextUnavailable(), nil, nil)
		_, ok := service.(auth.EmailInviter)
		if !ok {
			t.Fatalf("expected Service to be of type EmailInviter")
		}
	})
	t.Run("maintain_service", func(t *testing.T) {
		cfg := &config.ConfigImpl{}
		kvStore := kvtest.GetStore(t.Context(), t)
		meta := auth.NewKVMetadataManager("serve_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore)
		cfg.Auth.GetAuthUIConfig().RBAC = config.AuthRBACNone
		service := authfactory.NewAuthService(t.Context(), cfg, logging.ContextUnavailable(), kvStore, meta)
		_, ok := service.(auth.EmailInviter)
		if ok {
			t.Fatalf("expected Service to not be of type EmailInviter")
		}
	})
}
