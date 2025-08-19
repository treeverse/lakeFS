package awsiam

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestNewJWTCache(t *testing.T) {
	t.Run("with custom cache dir", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)
		require.NotEmpty(t, cache)
		require.Equal(t, filepath.Join(tempDir, lakectlDirName, cacheDirName, fileName), cache.filePath)
	})

	t.Run("with empty cache dir uses home dir", func(t *testing.T) {
		cache, err := NewJWTCache("")
		require.NoError(t, err)
		require.NotEmpty(t, cache)
		homeDir, _ := os.UserHomeDir()
		expectedPath := filepath.Join(homeDir, lakectlDirName, cacheDirName, fileName)
		require.Equal(t, expectedPath, cache.filePath)
	})
}

func TestJWTCacheSaveToken(t *testing.T) {
	t.Run("saves valid token successfully", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		token := &apigen.AuthenticationToken{
			Token:           "test-jwt-token",
			TokenExpiration: &expirationTime,
		}

		err = cache.SaveToken(token)
		require.NoError(t, err)

		// verify file exists and has correct permissions
		info, err := os.Stat(cache.filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())

		data, err := os.ReadFile(cache.filePath)
		require.NoError(t, err)

		var savedCache TokenCache
		err = json.Unmarshal(data, &savedCache)
		require.NoError(t, err)
		require.Equal(t, "test-jwt-token", savedCache.Token)
		require.Equal(t, expirationTime, savedCache.ExpirationTime)
	})

	t.Run("handles nil token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		err = cache.SaveToken(nil)
		require.NoError(t, err)

		// file should not be created
		_, err = os.Stat(cache.filePath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("handles token with empty string", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		token := &apigen.AuthenticationToken{
			Token: "",
		}

		err = cache.SaveToken(token)
		require.NoError(t, err)

		// file should not be created
		_, err = os.Stat(cache.filePath)
		require.True(t, os.IsNotExist(err))
	})

	t.Run("handles token without expiration", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		token := &apigen.AuthenticationToken{
			Token:           "test-token",
			TokenExpiration: nil,
		}

		err = cache.SaveToken(token)
		require.NoError(t, err)

		// file should not be created due to nil expiration
		_, err = os.Stat(cache.filePath)
		require.True(t, os.IsNotExist(err))
	})
}

func TestJWTCacheGetToken(t *testing.T) {
	t.Run("loads valid non-expired token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
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

	t.Run("returns error for expired token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		expiredTime := time.Now().Add(-1 * time.Hour).Unix()
		expiredCache := TokenCache{
			Token:          "expired-token",
			ExpirationTime: expiredTime,
		}

		// Write expired token directly to file
		file, err := os.OpenFile(cache.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		require.NoError(t, err)
		err = json.NewEncoder(file).Encode(expiredCache)
		require.NoError(t, err)
		file.Close()

		// load should return nil for expired token
		loadedToken, err := cache.GetToken()
		require.ErrorIs(t, err, ErrTokenExpired)
		require.Nil(t, loadedToken)
	})

	t.Run("returns error for expired cache", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		token := &apigen.AuthenticationToken{
			Token:           "test-jwt-token",
			TokenExpiration: &expirationTime,
		}

		err = cache.SaveToken(token)
		require.NoError(t, err)

		// manually modify the cache file to have an old WriteTime
		oldWriteTime := time.Now().Add(-MaxCacheTime - 1*time.Minute).Unix()

		expiredCache := TokenCache{
			Token:          "test-jwt-token",
			ExpirationTime: expirationTime,
			WriteTime:      oldWriteTime,
		}

		file, err := os.OpenFile(cache.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		require.NoError(t, err)
		err = json.NewEncoder(file).Encode(expiredCache)
		require.NoError(t, err)
		file.Close()

		// Load should return ErrCacheExpired
		loadedToken, err := cache.GetToken()
		require.ErrorIs(t, err, ErrCacheExpired)
		require.Nil(t, loadedToken)
	})

	t.Run("returns nil when cache file doesn't exist", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		loadedToken, err := cache.GetToken()
		require.Error(t, err)
		require.Nil(t, loadedToken)
	})

	t.Run("returns error for corrupted cache file", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Write invalid JSON
		err = os.WriteFile(cache.filePath, []byte("invalid json"), 0600)
		require.NoError(t, err)

		loadedToken, err := cache.GetToken()
		require.Error(t, err)
		require.Nil(t, loadedToken)
	})
}

func TestJWTCacheSaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewJWTCache(tempDir)
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
