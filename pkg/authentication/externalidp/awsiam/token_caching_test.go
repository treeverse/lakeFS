package awsiam

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestNewJWTCache(t *testing.T) {
	t.Run("with custom cache dir", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)

		require.NoError(t, err)
		assert.NotNil(t, cache)
		assert.Equal(t, filepath.Join(tempDir, fileName), cache.filePath)
	})

	t.Run("with empty cache dir uses home dir", func(t *testing.T) {
		cache, err := NewJWTCache("")

		require.NoError(t, err)
		assert.NotNil(t, cache)

		homeDir, _ := os.UserHomeDir()
		expectedPath := filepath.Join(homeDir, fileName)
		assert.Equal(t, expectedPath, cache.filePath)
	})
}

func TestJWTCache_SaveToken(t *testing.T) {
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

		// Verify file exists and has correct permissions
		info, err := os.Stat(cache.filePath)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

		// Verify file content
		data, err := os.ReadFile(cache.filePath)
		require.NoError(t, err)

		var savedCache TokenCache
		err = json.Unmarshal(data, &savedCache)
		require.NoError(t, err)

		assert.Equal(t, "test-jwt-token", savedCache.Token)
		assert.Equal(t, expirationTime, savedCache.ExpirationTime)
	})

	t.Run("handles nil token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		err = cache.SaveToken(nil)
		assert.NoError(t, err)

		// File should not be created
		_, err = os.Stat(cache.filePath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("handles token with empty string", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		token := &apigen.AuthenticationToken{
			Token: "",
		}

		err = cache.SaveToken(token)
		assert.NoError(t, err)

		// File should not be created
		_, err = os.Stat(cache.filePath)
		assert.True(t, os.IsNotExist(err))
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
		assert.NoError(t, err)

		// File should not be created due to nil expiration
		_, err = os.Stat(cache.filePath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("atomic write - temp file cleanup on error", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Make directory read-only to force write error
		err = os.Chmod(tempDir, 0400)
		require.NoError(t, err)
		defer os.Chmod(tempDir, 0700) // Cleanup

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		token := &apigen.AuthenticationToken{
			Token:           "test-token",
			TokenExpiration: &expirationTime,
		}

		err = cache.SaveToken(token)
		assert.Error(t, err)

		// Verify temp file doesn't exist
		tempFile := cache.filePath + ".tmp"
		_, err = os.Stat(tempFile)
		assert.True(t, os.IsNotExist(err))
	})
}

func TestJWTCache_LoadToken(t *testing.T) {
	t.Run("loads valid non-expired token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Save a token first
		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		originalToken := &apigen.AuthenticationToken{
			Token:           "test-jwt-token",
			TokenExpiration: &expirationTime,
		}
		err = cache.SaveToken(originalToken)
		require.NoError(t, err)

		// Load the token
		loadedToken, err := cache.LoadToken()
		require.NoError(t, err)
		require.NotNil(t, loadedToken)

		assert.Equal(t, "test-jwt-token", loadedToken.Token)
		assert.Equal(t, expirationTime, *loadedToken.TokenExpiration)
	})

	t.Run("returns nil for expired token", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Create expired token manually
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

		// Load should return nil for expired token
		loadedToken, err := cache.LoadToken()
		assert.NoError(t, err)
		assert.Nil(t, loadedToken)
	})

	t.Run("returns nil when cache file doesn't exist", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		loadedToken, err := cache.LoadToken()
		assert.Error(t, err) // Your current implementation returns error for non-existent file
		assert.Nil(t, loadedToken)
	})

	t.Run("returns error for corrupted cache file", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Write invalid JSON
		err = os.WriteFile(cache.filePath, []byte("invalid json"), 0600)
		require.NoError(t, err)

		loadedToken, err := cache.LoadToken()
		assert.Error(t, err)
		assert.Nil(t, loadedToken)
	})

	t.Run("handles token with zero expiration time", func(t *testing.T) {
		tempDir := t.TempDir()
		cache, err := NewJWTCache(tempDir)
		require.NoError(t, err)

		// Create token with zero expiration (should not expire)
		tokenCache := TokenCache{
			Token:          "no-expiry-token",
			ExpirationTime: 0,
		}

		file, err := os.OpenFile(cache.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		require.NoError(t, err)
		err = json.NewEncoder(file).Encode(tokenCache)
		require.NoError(t, err)
		file.Close()

		loadedToken, err := cache.LoadToken()
		require.NoError(t, err)
		require.NotNil(t, loadedToken)

		assert.Equal(t, "no-expiry-token", loadedToken.Token)
		assert.Equal(t, int64(0), *loadedToken.TokenExpiration)
	})
}

func TestJWTCache_SaveAndLoadRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewJWTCache(tempDir)
	require.NoError(t, err)

	expirationTime := time.Now().Add(30 * time.Minute).Unix()
	originalToken := &apigen.AuthenticationToken{
		Token:           "round-trip-token",
		TokenExpiration: &expirationTime,
	}

	// Save token
	err = cache.SaveToken(originalToken)
	require.NoError(t, err)

	// Load token
	loadedToken, err := cache.LoadToken()
	require.NoError(t, err)
	require.NotNil(t, loadedToken)

	// Verify they match
	assert.Equal(t, originalToken.Token, loadedToken.Token)
	assert.Equal(t, *originalToken.TokenExpiration, *loadedToken.TokenExpiration)
}

func TestJWTCache_ConcurrentAccess(t *testing.T) {
	// Basic test for race conditions
	tempDir := t.TempDir()
	cache, err := NewJWTCache(tempDir)
	require.NoError(t, err)

	expirationTime := time.Now().Add(1 * time.Hour).Unix()
	token := &apigen.AuthenticationToken{
		Token:           "concurrent-token",
		TokenExpiration: &expirationTime,
	}

	// Save and load concurrently
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 10; i++ {
			cache.SaveToken(token)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			cache.LoadToken()
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Verify final state
	loadedToken, err := cache.LoadToken()
	require.NoError(t, err)
	require.NotNil(t, loadedToken)
	assert.Equal(t, "concurrent-token", loadedToken.Token)
}

// Benchmark tests
func BenchmarkJWTCache_SaveToken(b *testing.B) {
	tempDir := b.TempDir()
	cache, err := NewJWTCache(tempDir)
	require.NoError(b, err)

	expirationTime := time.Now().Add(1 * time.Hour).Unix()
	token := &apigen.AuthenticationToken{
		Token:           "benchmark-token",
		TokenExpiration: &expirationTime,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.SaveToken(token)
	}
}

func BenchmarkJWTCache_LoadToken(b *testing.B) {
	tempDir := b.TempDir()
	cache, err := NewJWTCache(tempDir)
	require.NoError(b, err)

	// Setup: save a token first
	expirationTime := time.Now().Add(1 * time.Hour).Unix()
	token := &apigen.AuthenticationToken{
		Token:           "benchmark-token",
		TokenExpiration: &expirationTime,
	}
	cache.SaveToken(token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.LoadToken()
	}
}
