package auth

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	LoginAudience = "login"
)

// LoginClaims is a struct that implements jwt.Claims interface for login authentication
type LoginClaims struct {
	ID        string           `json:"jti,omitempty"`
	Issuer    string           `json:"iss,omitempty"`
	Subject   string           `json:"sub,omitempty"`
	Audience  string           `json:"aud,omitempty"`
	IssuedAt  *jwt.NumericDate `json:"iat,omitempty"`
	ExpiresAt *jwt.NumericDate `json:"exp,omitempty"`
}

// GetIssuer returns the claim's issuer
func (lc LoginClaims) GetIssuer() (string, error) {
	return lc.Issuer, nil
}

// GetSubject returns the claim's subject
func (lc LoginClaims) GetSubject() (string, error) {
	return lc.Subject, nil
}

// GetAudience returns the claim's audience
func (lc LoginClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{lc.Audience}, nil
}

// GetExpirationTime returns the claim's expiration time
func (lc LoginClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return lc.ExpiresAt, nil
}

// GetNotBefore returns the claim's not-before time
func (lc LoginClaims) GetNotBefore() (*jwt.NumericDate, error) {
	return nil, nil
}

// GetIssuedAt returns the claim's issued-at time
func (lc LoginClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return lc.IssuedAt, nil
}

func VerifyToken(secret []byte, tokenString string) (*LoginClaims, error) {
	claims := &LoginClaims{}
	keyFn := func(token *jwt.Token) (any, error) { return secret, nil }
	token, err := jwt.ParseWithClaims(tokenString, claims, keyFn,
		jwt.WithExpirationRequired(),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Name, jwt.SigningMethodHS384.Name, jwt.SigningMethodHS512.Name}),
	)
	if err != nil || !token.Valid {
		return nil, ErrInvalidToken
	}
	// Accept only LoginAudience or empty audience for backward compatibility
	if claims.Audience != LoginAudience && claims.Audience != "" {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// GenerateJWTLogin creates a jwt token which can be used for authentication during login only, i.e. it will not work for password reset.
// It supports backward compatibility for creating a login jwt. The audience is set for login token.
// No email is passed to support the ability of login for users via user/access keys which don't have an email yet
func GenerateJWTLogin(secret []byte, userID string, issuedAt, expiresAt time.Time) (string, error) {
	claims := LoginClaims{
		Issuer:    "auth",
		ID:        uuid.NewString(),
		Audience:  LoginAudience,
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(issuedAt),
		ExpiresAt: jwt.NewNumericDate(expiresAt),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}
