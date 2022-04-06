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

// Be aware! The aud field should *only* be used for new applications! Otherwise it should be passed as an empty string i.e. pass LoginAudience or the proper audience,
// so it supports backward compatibility. As well Id field contains the email and not the Id for backward compatibility. The secret field is the secret key
// used for encryption and set in the config file
func GenerateJWT(secret []byte, aud string, userID int, email string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Audience:  aud,
		Subject:   fmt.Sprint(userID),
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
		Id:        email,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
