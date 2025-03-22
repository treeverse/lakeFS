package auth

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

func VerifyToken(secret []byte, tokenString string) (*LoginClaims, error) {
	claims := &LoginClaims{}
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
