package api

import (
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
)

const (
	LoginAudience = "login"
)

// GenerateJWTLogin creates a jwt token which can be used for authentication during login only, i.e. it will not work for password reset.
// It supports backward compatibility for creating a login jwt. The audience is not set for login token. Any audience will make the token
// invalid for login. No email is passed to support the ability of login for users via user/access keys which don't have an email yet
func GenerateJWTLogin(secret []byte, userID string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		Id:        uuid.NewString(),
		Audience:  LoginAudience,
		Subject:   userID,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
