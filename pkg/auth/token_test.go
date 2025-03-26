package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const sampleRSAPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIICXwIBAAKBgQDN+wofEP/rjz8JaZz9vjuplDRoIMH0fEh2BMcbEkRLY9lbhNi/
w0if+ytwRkH7i4thX43IQ4+EhlmbgIzzUXQfXdAct2vNbiiFy0FyNIlz67Yknt2q
uHf2RgKGXu/vcMI7Dguyajo5mHwLiOoJ7ql86uNtquLiTvE5eLqd1gWRFQIDAQAB
AoGBALeT2qRveTdPFsZjy1hWuEPd44s+Tr6AGfCdN3rIH/f1CJ5JWwglms+Cgmdx
JpNy/gkNqYZnuDxLpQczXevpl4xXs5evn3mpeP668zLyzx0u6FJkS905MvOKF4vk
O+4eAa/12dpV7vEpWiLtZc3n+h4Y4L7EEluGR6VvwEbS/5cNAkEA5t5lhgL+x6u6
++hLOXZHg3bU7V5HfIdsoX8lAeZwgpvrcWzllZYoO7PQWaITM+ibcpjKQNCM3Jnx
iDBkfuW5fwJBAORnFmxkwZXOxfH4DwK+oup9x34RVMHXI9Y12xy5MOgKb5vgRDeN
el7m9DkyRYRo8TM4633gmTrFAx9Gogq5d2sCQQC6AqfjwJgMwmWmPzQUuSLHXkAS
e+q2/9nbiLiFfmhaI0wgmC+mRVRnPep5vWchZKGSRF54uE82EmaTZwIhZ+/7AkEA
sTXKkA8co77qlfKAswB2JrmwLoAD4uGpTGo8tux4pZBzR92ZEAEVEMzgcAAxL6q8
eaGQFPpN6OsyoPGMiAWeQQJBAIJjzp5vKxYAA6OZuc9h7mTChqGZWE8xWhT0qUxY
/nDqqxFYcBZ8BzD+Os3MF+Vnvewqmh4vZP69t8mVnuIF6Do=
-----END RSA PRIVATE KEY-----`

func TestGenerateJWTLogin(t *testing.T) {
	secret := []byte("test-secret-key")
	userID := "test-user"
	now := time.Now()
	expiresAt := now.Add(time.Hour)

	token, err := GenerateJWTLogin(secret, userID, now, expiresAt)
	require.NoError(t, err, "Failed to generate token")
	require.NotEmpty(t, token, "Generated token should not be empty")

	// Verify the generated token
	claims, err := VerifyToken(secret, token)
	require.NoError(t, err, "Failed to verify token")
	require.Equal(t, userID, claims.Subject, "Subject in claims should match userID")
	require.Equal(t, LoginAudience, claims.Audience, "Audience in claims should match LoginAudience")
	require.Equal(t, "auth", claims.Issuer, "Issuer should be 'auth'")

	// Check time claims
	issuedAt, err := claims.GetIssuedAt()
	require.NoError(t, err, "Failed to get IssuedAt")
	require.Equal(t, now.Unix(), issuedAt.Unix(), "IssuedAt should match creation time")

	expTime, err := claims.GetExpirationTime()
	require.NoError(t, err, "Failed to get ExpirationTime")
	require.Equal(t, expiresAt.Unix(), expTime.Unix(), "ExpiresAt should match expected expiration time")
}

func TestVerifyToken(t *testing.T) {
	secret := []byte("test-secret-key")
	userID := "test-user"
	now := time.Now()
	expiresAt := now.Add(time.Hour)

	// Generate a valid token
	token, err := GenerateJWTLogin(secret, userID, now, expiresAt)
	require.NoError(t, err, "Failed to generate token")

	// Test successful verification
	claims, err := VerifyToken(secret, token)
	require.NoError(t, err, "Failed to verify valid token")
	require.Equal(t, userID, claims.Subject, "Subject in claims should match userID")

	// Test verification with wrong secret
	wrongSecret := []byte("wrong-secret")
	_, err = VerifyToken(wrongSecret, token)
	require.Error(t, err, "Should fail with wrong secret")
	require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for wrong secret")

	// Test verification with invalid token
	_, err = VerifyToken(secret, "invalid-token")
	require.Error(t, err, "Should fail with invalid token")
	require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for invalid token")

	// Test verification with expired token
	expiredTime := now.Add(-2 * time.Hour)
	expiredToken, _ := GenerateJWTLogin(secret, userID, expiredTime, expiredTime.Add(time.Hour))
	_, err = VerifyToken(secret, expiredToken)
	require.Error(t, err, "Should fail with expired token")
	require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for expired token")
}

func TestVerifyToken_Specifics(t *testing.T) {
	secret := []byte("test-secret-key")
	userID := "test-user"
	now := time.Now()

	t.Run("expiration validation", func(t *testing.T) {
		// Test expired token
		expiredTime := now.Add(-2 * time.Hour)
		expiredToken, err := GenerateJWTLogin(secret, userID, expiredTime, expiredTime.Add(time.Hour))
		require.NoError(t, err, "Failed to generate expired token")

		_, err = VerifyToken(secret, expiredToken)
		require.Error(t, err, "Should fail with expired token")
		require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for expired token")

		// Test token with future expiration
		futureTime := now.Add(2 * time.Hour)
		validToken, err := GenerateJWTLogin(secret, userID, now, futureTime)
		require.NoError(t, err, "Failed to generate valid token")

		claims, err := VerifyToken(secret, validToken)
		require.NoError(t, err, "Should succeed with valid expiration")
		require.Equal(t, userID, claims.Subject, "Subject in claims should match userID")
	})

	t.Run("audience validation", func(t *testing.T) {
		// Create a valid token with login audience using GenerateJWTLogin
		validToken, err := GenerateJWTLogin(secret, userID, now, now.Add(time.Hour))
		require.NoError(t, err, "Failed to generate token with login audience")

		claims, err := VerifyToken(secret, validToken)
		require.NoError(t, err, "Should validate token with correct audience")
		require.Equal(t, LoginAudience, claims.Audience, "Audience in claims should match LoginAudience")

		// Create a custom token with invalid audience
		invalidAudienceToken := jwt.NewWithClaims(jwt.SigningMethodHS256, LoginClaims{
			Issuer:    "auth",
			ID:        uuid.NewString(),
			Audience:  "invalid-audience",
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		})
		invalidAudienceTokenString, err := invalidAudienceToken.SignedString(secret)
		require.NoError(t, err, "Failed to generate token with invalid audience")

		_, err = VerifyToken(secret, invalidAudienceTokenString)
		require.Error(t, err, "Should fail with invalid audience")
		require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for invalid audience")

		// Create a custom token with empty audience
		emptyAudienceToken := jwt.NewWithClaims(jwt.SigningMethodHS256, LoginClaims{
			Issuer:    "auth",
			ID:        uuid.NewString(),
			Audience:  "", // Empty audience
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		})
		emptyAudienceTokenString, err := emptyAudienceToken.SignedString(secret)
		require.NoError(t, err, "Failed to generate token with empty audience")

		// This should now pass since we're accepting empty audiences
		claims, err = VerifyToken(secret, emptyAudienceTokenString)
		require.NoError(t, err, "Should succeed with empty audience")
		require.Equal(t, "", claims.Audience, "Audience in claims should be empty")
	})

	t.Run("signing method validation", func(t *testing.T) {
		// Create a valid token with HS256
		validToken, err := GenerateJWTLogin(secret, userID, now, now.Add(time.Hour))
		require.NoError(t, err, "Failed to generate token with HS256 signing method")

		// Verify the token
		claims, err := VerifyToken(secret, validToken)
		require.NoError(t, err, "Should validate token with correct signing method")
		require.Equal(t, userID, claims.Subject, "Subject in claims should match userID")

		// Create a token with RS256 method
		invalidMethodToken := jwt.NewWithClaims(jwt.SigningMethodRS256, LoginClaims{
			Issuer:    "auth",
			ID:        uuid.NewString(),
			Audience:  LoginAudience,
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		})

		// generate rsa key pair
		rsaKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(sampleRSAPrivateKey))
		require.NoError(t, err, "Failed to parse RSA private key")

		invalidMethodTokenString, err := invalidMethodToken.SignedString(rsaKey)
		require.NoError(t, err, "Failed to generate token with invalid signing method")

		_, err = VerifyToken(secret, invalidMethodTokenString)
		require.Error(t, err, "Should fail with invalid signing method")
		require.Equal(t, ErrInvalidToken, err, "Should return ErrInvalidToken for invalid signing method")
	})
}

func TestLoginClaims_Methods(t *testing.T) {
	now := time.Now()
	expiry := now.Add(time.Hour)
	claims := LoginClaims{
		ID:        "test-id",
		Issuer:    "test-issuer",
		Subject:   "test-subject",
		Audience:  LoginAudience,
		IssuedAt:  &jwt.NumericDate{Time: now},
		ExpiresAt: &jwt.NumericDate{Time: expiry},
	}

	// Test GetIssuer
	issuer, err := claims.GetIssuer()
	require.NoError(t, err)
	require.Equal(t, "test-issuer", issuer)

	// Test GetSubject
	subject, err := claims.GetSubject()
	require.NoError(t, err)
	require.Equal(t, "test-subject", subject)

	// Test GetAudience
	audience, err := claims.GetAudience()
	require.NoError(t, err)
	require.Equal(t, 1, len(audience))
	require.Equal(t, LoginAudience, audience[0])

	// Test GetExpirationTime
	expTime, err := claims.GetExpirationTime()
	require.NoError(t, err)
	require.Equal(t, expiry.Unix(), expTime.Unix())

	// Test GetNotBefore (returns nil), not using it
	nbf, err := claims.GetNotBefore()
	require.NoError(t, err)
	require.Nil(t, nbf)

	// Test GetIssuedAt
	issuedAt, err := claims.GetIssuedAt()
	require.NoError(t, err)
	require.Equal(t, now.Unix(), issuedAt.Unix())
}
