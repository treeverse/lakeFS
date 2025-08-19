package awsiam

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var ErrTokenExpired = fmt.Errorf("token expired")
var ErrCacheExpired = fmt.Errorf("cache expired")
var ErrFailedToCreateCacheDir = fmt.Errorf("failed to create cache dir")

const (
	fileName           = "lakectl_token_cache.json"
	lakectlDirName     = ".lakectl"
	cacheDirName       = "cache"
	readWriteOwnerOnly = 0600
	MaxCacheTime       = 3600 * time.Second
)

type TokenCache struct {
	Token          string `json:"token"`
	ExpirationTime int64  `json:"expiration_time"`
	WriteTime      int64  `json:"write_time"`
}

type JWTCache struct {
	filePath string
}

func NewJWTCache(baseDir string) (*JWTCache, error) {
	if baseDir == "" {
		var err error
		baseDir, err = os.UserHomeDir()
		if err != nil {
			return nil, err
		}
	}
	cacheDir := filepath.Join(baseDir, lakectlDirName, cacheDirName)
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, ErrFailedToCreateCacheDir
	}

	jwtCache := &JWTCache{
		filePath: filepath.Join(cacheDir, fileName),
	}
	return jwtCache, nil
}

func (c *JWTCache) SaveToken(token *apigen.AuthenticationToken) error {
	if token == nil || token.Token == "" || token.TokenExpiration == nil {
		return nil
	}

	cache := &TokenCache{
		Token:          token.Token,
		ExpirationTime: *token.TokenExpiration,
		WriteTime:      time.Now().Unix(),
	}

	tmpFile := c.filePath + ".tmp"
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, readWriteOwnerOnly)
	if err != nil {
		return err
	}

	err = json.NewEncoder(file).Encode(cache)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	err = os.Rename(tmpFile, c.filePath)
	if err != nil {
		os.Remove(tmpFile)
	}
	return err
}

func (c *JWTCache) LoadToken(refreshInterval time.Duration) (*apigen.AuthenticationToken, error) {
	file, err := os.OpenFile(c.filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cache TokenCache

	err = json.NewDecoder(file).Decode(&cache)
	if err != nil {
		return nil, err
	}

	if cache.ExpirationTime > 0 && time.Now().Unix() >= cache.ExpirationTime+int64(refreshInterval.Seconds()) {
		return nil, ErrTokenExpired
	}

	if cache.WriteTime+int64(MaxCacheTime.Seconds()) <= time.Now().Unix() {
		return nil, ErrCacheExpired
	}

	token := &apigen.AuthenticationToken{
		Token:           cache.Token,
		TokenExpiration: &cache.ExpirationTime}
	return token, nil
}

func (c *JWTCache) ClearCache() error {
	return os.Remove(c.filePath)
}
