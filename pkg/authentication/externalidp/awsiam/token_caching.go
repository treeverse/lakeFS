package awsiam

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	fileName           = ".lakectl_token_cache.json"
	readWriteOwnerOnly = 0600
)

type TokenCache struct {
	Token          string `json:"token"`
	ExpirationTime int64  `json:"expiration_time"`
}

type JWTCache struct {
	filePath string
}

func NewJWTCache(cacheDir string) (*JWTCache, error) {
	if cacheDir == "" {
		var err error
		cacheDir, err = os.UserHomeDir()
		if err != nil {
			return nil, err
		}
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
	}

	tmpFile := c.filePath + ".tmp"
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, readWriteOwnerOnly)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(cache)
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
		return nil, nil
	}
	token := &apigen.AuthenticationToken{
		Token:           cache.Token,
		TokenExpiration: &cache.ExpirationTime}
	return token, nil
}

func (c *JWTCache) ClearCache() error {
	return os.Remove(c.filePath)
}
