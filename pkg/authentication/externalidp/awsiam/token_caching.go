package awsiam

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

var ErrFailedToCreateCacheDir = fmt.Errorf("failed to create cache dir")
var ErrInvalidTokenFormat = fmt.Errorf("token format is invalid")

const (
	readWriteOwnerOnly        = 0600
	ReadWriteExecuteOwnerOnly = 0700
	strLen                    = 8
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

	randomBytes := make([]byte, strLen)
	if _, err := rand.Read(randomBytes); err != nil {
		return err
	}
	randomSuffix := hex.EncodeToString(randomBytes)
	tmpFile := fmt.Sprintf("%s.tmp.%s", c.FilePath, randomSuffix)
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, readWriteOwnerOnly)
	if err != nil {
		return err
	}

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

	err = os.Rename(tmpFile, c.FilePath)
	if err != nil {
		os.Remove(tmpFile)
	}
	return err
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
