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
	user := &model.UserData{
		Username: "user-old",
	}
	userData, err := proto.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), model.UserPath(user.Username), userData))
	encryptedKey, err := model.EncryptSecret(s.SecretStore(), secretAccessKey)
	require.NoError(t, err)
	creds := &model.CredentialData{
		AccessKeyId:                   accessKeyID,
		SecretAccessKeyEncryptedBytes: encryptedKey,
		UserId:                        []byte(user.Username),
	}
	credsData, err := proto.Marshal(creds)
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), model.CredentialPath(user.Username, creds.AccessKeyId), credsData))

	creds.AccessKeyId = "A" + secretAccessKey
	encryptedKey, err = model.EncryptSecret(s.SecretStore(), "BadSecret")
	require.NoError(t, err)

	creds.SecretAccessKeyEncryptedBytes = encryptedKey
	credsData, err = proto.Marshal(creds)
	require.NoError(t, store.Set(ctx, []byte(model.PartitionKey), model.CredentialPath(user.Username, creds.AccessKeyId), credsData))

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
