package auth

import (
	"fmt"

	"github.com/golang-jwt/jwt"
)

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
