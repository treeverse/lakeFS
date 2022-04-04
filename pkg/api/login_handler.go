package api

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
)

type LoginRequestData struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

type LoginResponseData struct {
	Token string `json:"token"`
}

const (
	DefaultLoginExpiration = time.Hour * 24 * 7
	JWTCookieName          = "access_token"
)

// Be aware! The aud field should *only* be used for new applications! Otherwise it should be passed as an empty string, so it supports
// backward compatibility.

func GenerateJWT(aud string, secret []byte, userID int, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Audience:  aud,
		Subject:   fmt.Sprint(userID),
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
