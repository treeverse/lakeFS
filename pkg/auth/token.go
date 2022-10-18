package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/rs/xid"
)

const (
	ResetPasswordAudience = "reset_password"
)

func generateJWT(claims *jwt.StandardClaims, secret []byte) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// GenerateJWTResetPassword creates a jwt token with the field subject set the email passed.
func GenerateJWTResetPassword(secret []byte, email string, issuedAt, expiresAt time.Time) (string, error) {
	claims := &jwt.StandardClaims{
		// xid is k-sorted which is important functionality to ensure
		// easy deletion of tokens from the kv-store.
		Id:        xid.New().String(),
		Audience:  ResetPasswordAudience,
		Subject:   email,
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: expiresAt.Unix(),
	}
	return generateJWT(claims, secret)
}

func VerifyToken(secret []byte, tokenString string) (*jwt.StandardClaims, error) {
	claims := &jwt.StandardClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Header["alg"])
		}
		return secret, nil
	})
	if err != nil || !token.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

func VerifyTokenWithAudience(secret []byte, token, audience string) (*jwt.StandardClaims, error) {
	claims, err := VerifyToken(secret, token)
	if err != nil {
		return nil, err
	}
	if !claims.VerifyAudience(audience, true) {
		return nil, ErrInvalidToken
	}
	return claims, nil
}
