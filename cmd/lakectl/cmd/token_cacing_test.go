package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/authentication/externalidp/awsiam"
	"github.com/treeverse/lakefs/pkg/logging"
)

func resetGlobalState() {
	tokenLoadOnce = sync.Once{}
	tokenCacheOnce = sync.Once{}
	tokenSaveOnce = sync.Once{}
	cachedToken = nil
	tokenCache = nil
}

func TestGetTokenCacheOnce(t *testing.T) {
	t.Run("creates cache on first call ONLY", func(t *testing.T) {
		resetGlobalState()
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
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		token := getTokenOnce()
		require.Nil(t, token)
	})
}
func TestGetTokenOnce(t *testing.T) {
	t.Run("returns cached token on subsequent calls", func(t *testing.T) {
		resetGlobalState()
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
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()
		err := SaveTokenToCache()
		require.ErrorIs(t, err, ErrTokenUnavailable)
	})

	t.Run("returns error when cache is nil", func(t *testing.T) {
		resetGlobalState()
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
		resetGlobalState()
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
		resetGlobalState()
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
		resetGlobalState()
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

var errMockLoginFailed = fmt.Errorf("mock login failed")

type mockExternalLoginClient struct {
	shouldFail    bool
	loginCount    int64
	tokenToReturn string
}

func (m *mockExternalLoginClient) ExternalPrincipalLogin(ctx context.Context, loginInfo apigen.ExternalLoginInformation) (*apigen.AuthenticationToken, error) {
	atomic.AddInt64(&m.loginCount, 1)

	if m.shouldFail {
		return nil, errMockLoginFailed
	}

	expiry := time.Now().Add(1 * time.Hour).Unix()
	return &apigen.AuthenticationToken{
		Token:           m.tokenToReturn,
		TokenExpiration: &expiry,
	}, nil
}

func (m *mockExternalLoginClient) getLoginCount() int64 {
	return atomic.LoadInt64(&m.loginCount)
}

func (m *mockExternalLoginClient) resetLoginCount() {
	atomic.StoreInt64(&m.loginCount, 0)
}

func createTestTokenCacheCallback(callbackCount *int64) awsiam.TokenCacheCallback {
	return func(newToken *apigen.AuthenticationToken) {
		if callbackCount != nil {
			atomic.AddInt64(callbackCount, 1)
		}
		// update global cached token
		cachedToken = newToken
		if err := SaveTokenToCache(); err != nil {
			logging.ContextUnavailable().Debugf("error saving token to cache: %w", err)
		}
	}
}

func createSecurityProvider(mockClient *mockExternalLoginClient, initialToken *apigen.AuthenticationToken, callbackCount *int64) *awsiam.SecurityProviderAWSIAMRole {
	iamAuthParams := &awsiam.IAMAuthParams{
		RefreshInterval: 5 * time.Minute,
	}
	presignOpt := func(po *sts.PresignOptions) {
		po.ClientOptions = append(po.ClientOptions, func(o *sts.Options) {
			o.ClientLogMode = aws.LogSigning

		})
	}
	presignOpt2 := func(po *sts.PresignOptions) {
		po.ClientOptions = append(po.ClientOptions, func(o *sts.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider("fake-access", "fake-secret", "")
			o.Region = "us-east-1"
		})
	}

	tokenCacheCallback := createTestTokenCacheCallback(callbackCount)

	return awsiam.NewSecurityProviderAWSIAMRole(
		logging.Dummy(),
		iamAuthParams,
		mockClient,
		initialToken,
		tokenCacheCallback,
		presignOpt,
		presignOpt2,
	)
}

func TestLoginOnlyOnce(t *testing.T) {
	t.Run("login called only once across multiple requests", func(t *testing.T) {
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		mockClient := &mockExternalLoginClient{
			shouldFail:    false,
			tokenToReturn: "cached-token",
		}

		var callbackCount int64
		provider := createSecurityProvider(mockClient, nil, &callbackCount)

		// should trigger login
		req1 := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err := provider.Intercept(context.Background(), req1)
		require.NoError(t, err)
		require.Equal(t, "Bearer cached-token", req1.Header.Get("Authorization"))
		require.Equal(t, int64(1), mockClient.getLoginCount())
		require.Equal(t, int64(1), atomic.LoadInt64(&callbackCount))

		// should use cached token, no additional login
		req2 := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err = provider.Intercept(context.Background(), req2)
		require.NoError(t, err)
		require.Equal(t, "Bearer cached-token", req2.Header.Get("Authorization"))
		require.Equal(t, int64(1), mockClient.getLoginCount())       // Still 1, no new login
		require.Equal(t, int64(1), atomic.LoadInt64(&callbackCount)) // Still 1, no new callback
	})
}
func TestNoLoginWhenTokenIsGiven(t *testing.T) {
	t.Run("no login performed when valid token provided initially", func(t *testing.T) {
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		expiry := time.Now().Add(2 * time.Hour).Unix()
		existingToken := &apigen.AuthenticationToken{
			Token:           "pre-existing-token",
			TokenExpiration: &expiry,
		}

		mockClient := &mockExternalLoginClient{
			shouldFail:    false,
			tokenToReturn: "should-not-be-used",
		}

		var callbackCount int64
		provider := createSecurityProvider(mockClient, existingToken, &callbackCount)

		// Request should use existing token, no login
		req := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err := provider.Intercept(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "Bearer pre-existing-token", req.Header.Get("Authorization"))
		require.Equal(t, int64(0), mockClient.getLoginCount())       // No login called
		require.Equal(t, int64(0), atomic.LoadInt64(&callbackCount)) // No callback called
	})
}
func TestRealInterceptWithGlobalCache2(t *testing.T) {
	t.Run("handles login failure gracefully", func(t *testing.T) {
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		mockClient := &mockExternalLoginClient{
			shouldFail:    true,
			tokenToReturn: "",
		}

		var callbackCount int64
		provider := createSecurityProvider(mockClient, nil, &callbackCount)

		// Request should fail due to login failure
		req := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err := provider.Intercept(context.Background(), req)
		require.ErrorIs(t, err, errMockLoginFailed)
		require.Empty(t, req.Header.Get("Authorization"))
		require.Equal(t, int64(1), mockClient.getLoginCount())       // Login attempted once
		require.Equal(t, int64(0), atomic.LoadInt64(&callbackCount)) // No callback on failure
	})
}
func TestRealInterceptWithGlobalCache5(t *testing.T) {
	t.Run("token cached via callback and reused after provider recreation", func(t *testing.T) {
		resetGlobalState()
		cleanup := setupTestHomeDir(t)
		defer cleanup()

		mockClient := &mockExternalLoginClient{
			shouldFail:    false,
			tokenToReturn: "callback-cached-token",
		}

		var callbackCount int64
		provider1 := createSecurityProvider(mockClient, nil, &callbackCount)

		// First request - should trigger login and cache via callback
		req1 := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err := provider1.Intercept(context.Background(), req1)
		require.NoError(t, err)
		require.Equal(t, "Bearer callback-cached-token", req1.Header.Get("Authorization"))
		require.Equal(t, int64(1), mockClient.getLoginCount())
		require.Equal(t, int64(1), atomic.LoadInt64(&callbackCount))

		// Verify token was saved to global cache
		require.NotNil(t, cachedToken)
		require.Equal(t, "callback-cached-token", cachedToken.Token)

		// Create new provider instance (simulating new command execution)
		mockClient.resetLoginCount()
		callbackCount = 0

		// Get token from cache using global function
		cachedTokenFromFile := getTokenOnce()
		provider2 := createSecurityProvider(mockClient, cachedTokenFromFile, &callbackCount)

		// Second request with new provider should use cached token from file
		req2 := httptest.NewRequest("GET", "http://example.com/api/v1/repositories", nil)
		err = provider2.Intercept(context.Background(), req2)
		require.NoError(t, err)
		require.Equal(t, "Bearer callback-cached-token", req2.Header.Get("Authorization"))
		require.Equal(t, int64(0), mockClient.getLoginCount())       // No new login, used cache
		require.Equal(t, int64(0), atomic.LoadInt64(&callbackCount)) // No new callback
	})
}
