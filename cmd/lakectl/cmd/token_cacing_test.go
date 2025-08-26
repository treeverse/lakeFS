package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/authentication/externalidp/awsiam"
)

func TestGetTokenCacheOnce(t *testing.T) {
	t.Run("creates cache on first call ONLY", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		cache1 := getTokenCacheOnce()
		require.NotNil(t, cache1)

		// set $HOME on another dir
		cleanup2 := setupTestHomeDir(t)
		defer cleanup2()

		// Second call should return the same instance
		cache2 := getTokenCacheOnce()
		require.Equal(t, cache1, cache2)
	})
}

func TestGetTokenOnceNil(t *testing.T) {
	t.Run("returns nil when cache is empty", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		token := getTokenOnce()
		require.Nil(t, token)
	})
}
func TestGetTokenOnce(t *testing.T) {
	t.Run("returns cached token on subsequent calls", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		testToken := &apigen.AuthenticationToken{
			Token:           "test-cached-token",
			TokenExpiration: &expirationTime,
		}
		cache := getTokenCacheOnce()
		saveTokenToCacheOnce(cache, testToken)

		token1 := getTokenOnce()
		token2 := getTokenOnce()

		require.Equal(t, token1, token2)
		require.Equal(t, "test-cached-token", token1.Token)
	})
}

func TestSaveTokenToCacheFail(t *testing.T) {
	t.Run("returns error when no cached token", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()
		err := SaveTokenToCache()
		require.ErrorIs(t, err, ErrTokenUnavailable)
	})

	t.Run("returns error when cache is nil", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()
		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		cachedToken = &apigen.AuthenticationToken{
			Token:           "test-token",
			TokenExpiration: &expirationTime,
		}

		// force cache to be nil by setting it directly
		tokenCache = nil
		tokenCacheOnce.Do(func() {})

		err := SaveTokenToCache()
		require.ErrorIs(t, err, ErrTokenUnavailable)
	})
}
func TestSaveTokenToCache(t *testing.T) {
	t.Run("saves to cache only the first token", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		cache := getTokenCacheOnce()
		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		token := &apigen.AuthenticationToken{
			Token:           "save-test-token",
			TokenExpiration: &expirationTime,
		}
		token2 := &apigen.AuthenticationToken{
			Token:           "save-test-token2",
			TokenExpiration: &expirationTime,
		}

		// save both tokens, only first one should be saved.
		saveTokenToCacheOnce(cache, token)
		saveTokenToCacheOnce(cache, token2)

		savedTokend := getTokenOnce()
		require.Equal(t, "save-test-token", savedTokend.Token)
	})
}

func TestConcurrentAccess(t *testing.T) {
	t.Run("sync.Once ensures single execution under concurrency", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		const numGoroutines = 10
		var wg sync.WaitGroup

		cache := getTokenCacheOnce()
		require.NotNil(t, cache)

		expirationTime := time.Now().Add(1 * time.Hour).Unix()
		loginToken := &apigen.AuthenticationToken{
			Token:           "workflow-token",
			TokenExpiration: &expirationTime,
		}
		saveTokenToCacheOnce(cache, loginToken)

		results := make([]*awsiam.JWTCache, numGoroutines)

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx] = getTokenCacheOnce()
			}(i)
		}
		wg.Wait()

		firstResult := results[0]
		for i := 1; i < numGoroutines; i++ {
			require.Equal(t, firstResult, results[i])
		}
	})
}

func TestTokenExpiredWrite(t *testing.T) {
	t.Run("cache token that was written over an hour ago, should return nil", func(t *testing.T) {
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		cache := getTokenCacheOnce()
		require.NotNil(t, cache)

		expiredTime := time.Now().Add(1 * time.Hour).Unix()
		oldWriteTime := time.Now().Add(-2 * time.Hour).Unix()

		expiredTokenCache := awsiam.TokenCache{
			Token:          "old-written-token",
			ExpirationTime: expiredTime,
			WriteTime:      oldWriteTime,
		}

		// write directly to cache file to simulate old write time
		cacheFile := filepath.Join(os.Getenv("HOME"), ".lakectl", "cache", "lakectl_token_cache.json")
		err := os.MkdirAll(filepath.Dir(cacheFile), 0755)
		require.NoError(t, err)

		cacheData, err := json.Marshal(expiredTokenCache)
		require.NoError(t, err)

		err = os.WriteFile(cacheFile, cacheData, 0644)
		require.NoError(t, err)

		// try to get token - should fail due to old write time
		token := getTokenOnce()
		require.Nil(t, token)
	})
}

func setupTestHomeDir(t *testing.T) (cleanup func()) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")

	// set temp directory as home for both Unix and Windows
	os.Setenv("HOME", tempDir)
	os.Setenv("USERPROFILE", tempDir)

	return func() {
		if originalHome != "" {
			os.Setenv("HOME", originalHome)
		} else {
			os.Unsetenv("HOME")
		}
		os.Unsetenv("USERPROFILE")
	}
}
