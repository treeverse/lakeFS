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

func generateJWT(claims *jwt.StandardClaims, secret []byte) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// GenerateJWTLogin creates a jwt token which can be used for authentication during login only, i.e. it will not work for password reset.
// It supports backward compatibility for creating a login jwt. The audience is set to LoginAudience which is
// an empty string and no email is passed to support the ability of login for users via user/access keys which don't have an email set
func GenerateJWTLogin(secret []byte, userID int, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Audience:  LoginAudience,
		Subject:   fmt.Sprint(userID),
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	return generateJWT(claims, secret)
}

// GenerateJWTResetPassword creates a jwt token with the field subject set the email passed.
func GenerateJWTResetPassword(secret []byte, userID int, email string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Audience:  ResetPasswordAudience,
		Subject:   email,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
		Id:        fmt.Sprint(userID),
	}
	return generateJWT(claims, secret)
}
