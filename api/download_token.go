package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/auth/model"

	"github.com/treeverse/lakefs/logging"

	"errors"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth"
)

const (
	TokenFreshnessSeconds = 60 * 60 * 4 // 4 hours
)

var (
	ErrBadToken = errors.New("invalid token")
)

func ValidateToken(ath auth.Service, token string, currentTime time.Time) (*models.User, error) {
	lg := logging.Default().WithField("token", token)
	parts := strings.SplitN(token, "/", 4)
	if len(parts) != 4 {
		lg.Error("wrong format")
		return nil, ErrBadToken
	}

	signature := parts[0]
	ts := parts[1]
	accessKey := parts[2]
	path := parts[3]

	// parse and validate timestamp
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		lg.WithField("ts", ts).Error("date not parseable")
		return nil, ErrBadToken
	}

	tokenTime := time.Unix(tsInt, 0)
	if currentTime.Sub(tokenTime).Seconds() > TokenFreshnessSeconds {
		lg.WithField("ts", ts).WithField("seconds_old", currentTime.Sub(tokenTime).Seconds()).Error("date not fresh enough")
		return nil, ErrBadToken
	}

	// ensure access key is valid
	creds, err := ath.GetAPICredentials(accessKey)
	if err != nil {
		lg.WithField("access_key", accessKey).Error("could not get access key")
		return nil, ErrBadToken
	}

	// get secret
	secret := creds.AccessSecretKey

	// compare hmac
	sigBytes, err := hex.DecodeString(signature)
	if err != nil {
		lg.WithField("signature", signature).Error("could not decode signature hex")
		return nil, ErrBadToken
	}
	computedSig, err := hex.DecodeString(Generate(accessKey, secret, path, tokenTime))
	if err != nil {
		lg.Error("could not generate signature")
		return nil, ErrBadToken
	}

	if !hmac.Equal(sigBytes, computedSig) {
		lg.WithField("signature", signature).WithField("computed", computedSig).Error("signature mismatch")
		return nil, ErrBadToken
	}

	// return user
	if creds.Type != model.CredentialTypeUser {
		lg.Warn("could not get user: this is not a user credential type")
		return nil, ErrBadToken
	}
	user, err := ath.GetUser(*creds.UserId)
	if err != nil {
		lg.WithField("user", creds.UserId).Error("could not get user")
		return nil, ErrBadToken
	}

	return &models.User{
		Email:    user.Email,
		FullName: user.FullName,
		ID:       int64(user.Id),
	}, nil
}

func Generate(accessKey, secret, path string, timestamp time.Time) string {
	token := fmt.Sprintf("%d/%s/%s", timestamp.Unix(), accessKey, path)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(token))
	signature := h.Sum(nil)
	return fmt.Sprintf("%x", signature)
}
