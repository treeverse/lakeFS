package awsiam

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var ErrFailedToCreateCacheDir = errors.New("failed to create cache dir")
var ErrInvalidTokenFormat = errors.New("token format is invalid")

const (
	ReadWriteExecuteOwnerOnly = 0700
)

type TokenCache struct {
	Token          string `json:"token"`
	ExpirationTime int64  `json:"expiration_time"`
}

type JWTCache struct {
	FilePath string
}

func NewJWTCache(baseDir, lakectlDir, cacheDir, fileName string) (*JWTCache, error) {
	if baseDir == "" {
		var err error
		baseDir, err = os.UserHomeDir()
		if err != nil {
			return nil, err
		}
	}
	cachePath := filepath.Join(baseDir, lakectlDir, cacheDir)
	if err := os.MkdirAll(cachePath, ReadWriteExecuteOwnerOnly); err != nil {
		return nil, ErrFailedToCreateCacheDir
	}

	jwtCache := &JWTCache{
		FilePath: filepath.Join(cachePath, fileName),
	}
	return jwtCache, nil
}

func (c *JWTCache) SaveToken(token *apigen.AuthenticationToken) error {
	if token == nil || token.Token == "" || token.TokenExpiration == nil {
		return ErrInvalidTokenFormat
	}

	cache := &TokenCache{
		Token:          token.Token,
		ExpirationTime: *token.TokenExpiration,
	}

	dir, name := filepath.Split(c.FilePath)
	file, err := os.CreateTemp(dir, name+".*.tmp")
	if err != nil {
		return err
	}
	tmpFile := file.Name()

	err = json.NewEncoder(file).Encode(cache)
	if err != nil {
		file.Close()
		os.Remove(tmpFile)
		return err
	}

	err = file.Close()
	if err != nil {
		os.Remove(tmpFile)
		return err
	}

	err = os.Rename(file.Name(), c.FilePath)
	if err != nil {
		os.Remove(tmpFile)
		return err
	}
	return nil
}

func (c *JWTCache) GetToken() (*apigen.AuthenticationToken, error) {
	file, err := os.OpenFile(c.FilePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cache TokenCache

	err = json.NewDecoder(file).Decode(&cache)
	if err != nil {
		return nil, err
	}
	token := &apigen.AuthenticationToken{
		Token:           cache.Token,
		TokenExpiration: &cache.ExpirationTime}
	return token, nil
}

func (c *JWTCache) ClearCache() error {
	return os.Remove(c.FilePath)
}
