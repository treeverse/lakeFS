package api

import (
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go"
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

func GenerateJWT(secret []byte, userID int, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Subject:   fmt.Sprint(userID),
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
