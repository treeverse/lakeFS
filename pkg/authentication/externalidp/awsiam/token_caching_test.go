package awsiam_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/authentication/externalidp/awsiam"
)

func TestNewJWTCache(t *testing.T) {
	t.Run("with custom cache dir", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)
		require.NotEmpty(t, cache)
		require.Equal(t, filepath.Join(tempDir,  ".lakectl_token_cache.json"), cache.FilePath)
	})

	t.Run("with empty cache dir uses home dir", func(t *testing.T) {
		cache, err := awsiam.NewJWTCache("", ".lakectl_token_cache.json")
		require.NoError(t, err)
		require.NotEmpty(t, cache)
		homeDir, _ := os.UserHomeDir()
		expectedPath := filepath.Join(homeDir,  ".lakectl_token_cache.json")
		require.Equal(t, expectedPath, cache.FilePath)
	})
}

func TestJWTCacheSaveToken(t *testing.T) {
	t.Run("saves valid token successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		token := &apigen.AuthenticationToken{
			Token:           "test-jwt-token",
			TokenExpiration: &expirationTime,
		}

		err = cache.SaveToken(token)
		require.NoError(t, err)

		// verify file exists and has correct permissions
		info, err := os.Stat(cache.FilePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())

		data, err := os.ReadFile(cache.FilePath)
		require.NoError(t, err)

		var savedCache awsiam.TokenCache
		err = json.Unmarshal(data, &savedCache)
		require.NoError(t, err)
		require.Equal(t, "test-jwt-token", savedCache.Token)
		require.Equal(t, expirationTime, savedCache.ExpirationTime)
	})

	t.Run("handles nil token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		err = cache.SaveToken(nil)
		require.ErrorIs(t, err, awsiam.ErrInvalidTokenFormat)

		// file should not be created
		_, err = os.Stat(cache.FilePath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("handles token with empty string", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		token := &apigen.AuthenticationToken{
			Token: "",
		}

		err = cache.SaveToken(token)
		require.ErrorIs(t, err, awsiam.ErrInvalidTokenFormat)

		// file should not be created
		_, err = os.Stat(cache.FilePath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("handles token without expiration", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		token := &apigen.AuthenticationToken{
			Token:           "test-token",
			TokenExpiration: nil,
		}

		err = cache.SaveToken(token)
		require.ErrorIs(t, err, awsiam.ErrInvalidTokenFormat)

		// file should not be created due to nil expiration
		_, err = os.Stat(cache.FilePath)
		require.True(t, os.IsNotExist(err))
	})
}

func TestJWTCacheGetToken(t *testing.T) {
	t.Run("loads valid non-expired token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		originalToken := &apigen.AuthenticationToken{
			Token:           "test-jwt-token",
			TokenExpiration: &expirationTime,
		}
		err = cache.SaveToken(originalToken)
		require.NoError(t, err)

		// Load the token
		loadedToken, err := cache.GetToken()
		require.NoError(t, err)
		require.NotNil(t, loadedToken)

		require.Equal(t, "test-jwt-token", loadedToken.Token)
		require.Equal(t, expirationTime, *loadedToken.TokenExpiration)
	})

	t.Run("returns nil when cache file doesn't exist", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		loadedToken, err := cache.GetToken()
		require.Error(t, err)
		require.Nil(t, loadedToken)
	})

	t.Run("returns error for corrupted cache file", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
		require.NoError(t, err)

		// Write invalid JSON
		err = os.WriteFile(cache.FilePath, []byte("invalid json"), 0600)
		require.NoError(t, err)

		loadedToken, err := cache.GetToken()
		require.Error(t, err)
		require.Nil(t, loadedToken)
	})
}

func TestJWTCacheSaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := awsiam.NewJWTCache(tempDir, ".lakectl_token_cache.json")
	require.NoError(t, err)

	expirationTime := time.Now().Add(30 * time.Minute).Unix()
	originalToken := &apigen.AuthenticationToken{
		Token:           "round-trip-token",
		TokenExpiration: &expirationTime,
	}

	err = cache.SaveToken(originalToken)
	require.NoError(t, err)

	loadedToken, err := cache.GetToken()
	require.NoError(t, err)
	require.NotNil(t, loadedToken)

	// verify they match
	require.Equal(t, originalToken.Token, loadedToken.Token)
	require.Equal(t, *originalToken.TokenExpiration, *loadedToken.TokenExpiration)
}
