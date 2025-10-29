package setup_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func SetupService(t *testing.T, secret string) (*auth.BasicAuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(context.Background(), t)
	return auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte(secret)), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable()), kvStore
}

func TestAddAdminUser(t *testing.T) {
	authService, _ := SetupService(t, "secret")
	ctx := context.Background()

	// setup admin user
	superuser := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  "testuser",
		},
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}
	_, err := setup.AddAdminUser(ctx, authService, superuser, false)
	if err != nil {
		t.Fatal("failed to add admin user:", err)
	}

	// try to add another admin user - should fail
	superuser2 := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  "testuser2",
		},
		AccessKeyID:     "key2",
		SecretAccessKey: "secret2",
	}
	_, err = setup.AddAdminUser(ctx, authService, superuser2, false)
	expectedErr := auth.ErrAlreadyExists
	if !errors.Is(err, expectedErr) {
		t.Fatalf("adding another admin. err:%v, expected:%v", err, expectedErr)
	}

	// delete admin user should work
	err = authService.DeleteUser(ctx, superuser.Username)
	if err != nil {
		t.Fatal("failed to delete admin user:", err)
	}

	// setup admin user again without secret access key should fail
	superuser.SecretAccessKey = ""
	_, err = setup.AddAdminUser(ctx, authService, superuser, false)
	expectedErr = auth.ErrNotFound
	if !errors.Is(err, expectedErr) {
		t.Fatalf("adding admin without secret access. key err:%v, expected:%v", err, expectedErr)
	}

	// we should be able to re-add the admin user
	superuser.SecretAccessKey = "new-secret"
	_, err = setup.AddAdminUser(ctx, authService, superuser, false)
	if err != nil {
		t.Fatal("failed to re-add admin user:", err)
	}
}
