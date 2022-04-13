package api

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
)

type LoginRequestData struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

type LoginResponseData struct {
	Token string `json:"token"`
}

const (
	DefaultLoginExpiration         = 7 * 24 * time.Hour
	DefaultResetPasswordExpiration = 6 * time.Hour

	JWTCookieName = "access_token"

	LoginAudience         = "login"
	ResetPasswordAudience = "reset_password"
)

func generateJWT(claims *jwt.StandardClaims, secret []byte) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// GenerateJWTLogin creates a jwt token which can be used for authentication during login only, i.e. it will not work for password reset.
// It supports backward compatibility for creating a login jwt. The audience is not set for login token. Any audience will make the token
// invalid for login. No email is passed to support the ability of login for users via user/access keys which don't have an email yet
func GenerateJWTLogin(secret []byte, userID int64, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Id:        uuid.NewString(),
		Audience:  LoginAudience,
		Subject:   fmt.Sprint(userID),
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	return generateJWT(claims, secret)
}

// GenerateJWTResetPassword creates a jwt token with the field subject set the email passed.
func GenerateJWTResetPassword(secret []byte, email string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Id:        uuid.NewString(),
		Audience:  ResetPasswordAudience,
		Subject:   email,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	return generateJWT(claims, secret)
}
