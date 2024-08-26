package auth_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

var secret string = "Secret"

func SetupService(t *testing.T, secret string) (*auth.BasicAuthService, kv.Store) {
	t.Helper()
	kvStore := kvtest.GetStore(context.Background(), t)
	return auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte(secret)), authparams.ServiceCache{
		Enabled: false,
	}, logging.ContextUnavailable()), kvStore
}

func TestBasicAuthService_Users(t *testing.T) {
	ctx := context.Background()
	s, store := SetupService(t, secret)
	username := "testUser"

	// Get user not exists
	_, err := s.GetUser(ctx, username)
	require.ErrorIs(t, err, auth.ErrNotFound)

	// List users with no users
	listRes, _, err := s.ListUsers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(listRes))

	// Delete no user
	err = s.DeleteUser(ctx, username)
	require.ErrorIs(t, err, auth.ErrNotFound)

	user := &model.User{
		Username: username,
	}
	createRes, err := s.CreateUser(ctx, user)
	require.NoError(t, err)
	require.Equal(t, username, createRes)

	// Check get user
	getRes, err := s.GetUser(ctx, user.Username)
	require.NoError(t, err)
	require.Equal(t, user.Username, getRes.Username)

	// Check it is saved under the admin key
	_, err = store.Get(ctx, []byte(auth.BasicPartitionKey), model.UserPath(auth.SuperAdminKey))
	require.NoError(t, err)

	// List users
	listRes, _, err = s.ListUsers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(listRes))
	require.Equal(t, username, listRes[0].Username)

	// Delete user
	err = s.DeleteUser(ctx, username)
	require.NoError(t, err)

	// Check admin key is deleted
	_, err = store.Get(ctx, []byte(auth.BasicPartitionKey), model.UserPath(auth.SuperAdminKey))
	require.ErrorIs(t, err, auth.ErrNotFound)
}

func TestBasicAuthService_Credentials(t *testing.T) {
	ctx := context.Background()
	s, _ := SetupService(t, secret)
	username := "testUser"
	accessKeyID := "SomeAccessKeyID"
	secretAccessKey := "SomeSecretAccessKey"

	// Get credentials no user
	_, err := s.GetCredentials(ctx, username)
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Create credentials no user
	_, err = s.CreateCredentials(ctx, username)
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Add credentials no user
	_, err = s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
	require.ErrorIs(t, err, auth.ErrNotFound)

	user := &model.User{
		Username: username,
	}
	createRes, err := s.CreateUser(ctx, user)
	require.NoError(t, err)
	require.Equal(t, username, createRes)

	// Get credentials (no creds)
	_, err = s.GetCredentials(ctx, accessKeyID)
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Create credentials for user
	creds, err := s.CreateCredentials(ctx, username)
	require.NoError(t, err)

	// Get credentials
	_, err = s.GetCredentials(ctx, creds.AccessKeyID)
	require.NoError(t, err)

	// Add credentials already exists
	_, err = s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
	require.ErrorIs(t, err, auth.ErrInvalidRequest)
}

func TestBasicAuthService_CredentialsImport(t *testing.T) {
	ctx := context.Background()
	s, store := SetupService(t, secret)
	username := "testUser"
	accessKeyID := "SomeAccessKeyID"
	secretAccessKey := "SomeSecretAccessKey"

	// Import credentials no user
	_, err := s.AddCredentials(ctx, username, accessKeyID, "")
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Create users with creds under auth
	user := createOldUser(t, ctx, store, "user-old")
	createOldCreds(t, ctx, store, s, user.Username, accessKeyID, secretAccessKey)
	createOldCreds(t, ctx, store, s, user.Username, "A"+accessKeyID, "BadSecret")

	// Create a different user
	createRes, err := s.CreateUser(ctx, &model.User{
		Username: username,
	})
	require.NoError(t, err)
	require.Equal(t, username, createRes)

	// Try to import credentials of different user
	_, err = s.AddCredentials(ctx, username, accessKeyID, "")
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Delete user and create the right one this time
	require.NoError(t, s.DeleteUser(ctx, username))
	_, err = s.CreateUser(ctx, &model.User{
		Username: user.Username,
	})
	require.NoError(t, err)

	// Import credentials not exist
	_, err = s.AddCredentials(ctx, user.Username, "NotExisting", "")
	require.ErrorIs(t, err, auth.ErrNotFound)

	// Import credentials
	credsResp, err := s.AddCredentials(ctx, user.Username, accessKeyID, "")
	require.NoError(t, err)
	require.Equal(t, accessKeyID, credsResp.AccessKeyID)
	require.Equal(t, secretAccessKey, credsResp.SecretAccessKey)

	// Import after exists
	_, err = s.AddCredentials(ctx, user.Username, accessKeyID, "")
	require.ErrorIs(t, err, auth.ErrInvalidRequest)

	// Get credentials and verify
	getCred, err := s.GetCredentials(ctx, accessKeyID)
	require.NoError(t, err)
	require.Equal(t, accessKeyID, getCred.AccessKeyID)
	require.Equal(t, secretAccessKey, getCred.SecretAccessKey)
}

func TestBasicAuthService_Migrate(t *testing.T) {
	ctx := context.Background()
	accessKeyID := "SomeAccessKeyID"
	secretAccessKey := "SomeSecretAccessKey"

	t.Run("no users", func(t *testing.T) {
		s, _ := SetupService(t, secret)
		_, err := s.Migrate(ctx)
		require.ErrorIs(t, err, auth.ErrMigrationNotPossible)
	})

	t.Run("superadmin exists", func(t *testing.T) {
		s, store := SetupService(t, secret)

		expectedUser, err := s.CreateUser(ctx, &model.User{Username: "test"})
		require.NoError(t, err)

		// create a user for potential migration
		createOldUser(t, ctx, store, "unexpected")
		createOldCreds(t, ctx, store, s, "unexpected", accessKeyID, secretAccessKey)

		// Should not run migration flow
		username, err := s.Migrate(ctx)
		require.NoError(t, err)
		require.Equal(t, "", username) // No migration so username is empty

		// Verify user didn't change
		user, err := s.GetUser(ctx, expectedUser)
		require.NoError(t, err)
		require.Equal(t, expectedUser, user.Username)
	})

	t.Run("too many users", func(t *testing.T) {
		s, store := SetupService(t, secret)

		createOldUser(t, ctx, store, "user1")
		createOldUser(t, ctx, store, "user2")

		_, err := s.Migrate(ctx)
		require.ErrorIs(t, err, auth.ErrMigrationNotPossible)
		require.Contains(t, err.Error(), "too many users")
	})

	t.Run("too many creds", func(t *testing.T) {
		s, store := SetupService(t, secret)

		user := createOldUser(t, ctx, store, "user1")
		createOldCreds(t, ctx, store, s, user.Username, "key1", "secret1")
		createOldCreds(t, ctx, store, s, user.Username, "key2", "secret2")

		_, err := s.Migrate(ctx)
		require.ErrorIs(t, err, auth.ErrMigrationNotPossible)
		require.Contains(t, err.Error(), "too many credentials")
	})

	t.Run("successful migration", func(t *testing.T) {
		s, store := SetupService(t, secret)

		expectedUser := createOldUser(t, ctx, store, "old-user")
		createOldCreds(t, ctx, store, s, expectedUser.Username, accessKeyID, secretAccessKey)
		username, err := s.Migrate(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedUser.Username, username)

		user, err := s.GetUser(ctx, expectedUser.Username)
		require.NoError(t, err)
		require.Equal(t, expectedUser.Username, user.Username)

		creds, err := s.GetCredentials(ctx, accessKeyID)
		require.NoError(t, err)
		require.Equal(t, creds.Username, expectedUser.Username)
		decryptedKey, err := model.DecryptSecret(s.SecretStore(), creds.SecretAccessKeyEncryptedBytes)
		require.NoError(t, err)
		require.Equal(t, secretAccessKey, decryptedKey)
	})
}

func createOldUser(t *testing.T, ctx context.Context, store kv.Store, username string) *model.UserData {
	t.Helper()
	user := &model.UserData{
		Username: username,
	}
	userData, err := proto.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), model.UserPath(user.Username), userData))
	return user
}

func createOldCreds(t *testing.T, ctx context.Context, store kv.Store, s *auth.BasicAuthService, username, accessKeyID, secretAccessKey string) *model.CredentialData {
	t.Helper()
	encryptedKey, err := model.EncryptSecret(s.SecretStore(), secretAccessKey)
	require.NoError(t, err)
	creds := &model.CredentialData{
		AccessKeyId:                   accessKeyID,
		SecretAccessKeyEncryptedBytes: encryptedKey,
		UserId:                        []byte(username),
	}
	credsData, err := proto.Marshal(creds)
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), model.CredentialPath(username, creds.AccessKeyId), credsData))
	return creds
}
