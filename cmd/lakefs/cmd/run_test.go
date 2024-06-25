package cmd_test

import (
	"context"
	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"testing"
)

func TestGetAuthService(t *testing.T) {
	t.Run("maintain_inviter", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Auth.API.Endpoint = "http://localhost:8000"
		cfg.Auth.API.SkipHealthCheck = true
		service := cmd.NewAuthService(context.Background(), cfg, logging.ContextUnavailable(), nil)
		_, ok := service.(auth.EmailInviter)
		if !ok {
			t.Fatalf("expected Service to be of type EmailInviter")
		}
	})
	t.Run("maintain_service", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.Auth.UIConfig.RBAC = config.AuthRBACSimplified
		service := cmd.NewAuthService(context.Background(), cfg, logging.ContextUnavailable(), nil)
		_, ok := service.(auth.EmailInviter)
		if ok {
			t.Fatalf("expected Service to not be of type EmailInviter")
		}
	})
}
