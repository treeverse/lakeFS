package cmd_test

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func TestGetAuthService(t *testing.T) {
	t.Run("maintain_inviter", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACInternal
		cfg.Auth.API.Endpoint = "http://localhost:8000"
		cfg.Auth.API.SkipHealthCheck = true
		service := cmd.NewAuthService(context.Background(), cfg, logging.ContextUnavailable(), nil, nil)
		_, ok := service.(auth.EmailInviter)
		if !ok {
			t.Fatalf("expected Service to be of type EmailInviter")
		}
	})
	t.Run("maintain_service", func(t *testing.T) {
		cfg := &config.Config{}
		kvStore := kvtest.GetStore(context.Background(), t)
		meta := auth.NewKVMetadataManager("serve_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore)
		cfg.Auth.UIConfig.RBAC = config.AuthRBACNone
		service := cmd.NewAuthService(context.Background(), cfg, logging.ContextUnavailable(), kvStore, meta)
		_, ok := service.(auth.EmailInviter)
		if ok {
			t.Fatalf("expected Service to not be of type EmailInviter")
		}
	})
}
