package jwtidp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/authentication/jwtidp"
	"github.com/treeverse/lakefs/pkg/config"
)

type signingSet struct {
	priv *rsa.PrivateKey
	kid  string
	pub  jose.JSONWebKey
}

func newSigningSet(t *testing.T, kid string) signingSet {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return signingSet{
		priv: priv,
		kid:  kid,
		pub: jose.JSONWebKey{
			Key:       &priv.PublicKey,
			KeyID:     kid,
			Algorithm: string(jose.RS256),
			Use:       "sig",
		},
	}
}

func (s signingSet) sign(t *testing.T, claims any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.RS256,
			Key: jose.JSONWebKey{
				Key:       s.priv,
				KeyID:     s.kid,
				Algorithm: string(jose.RS256),
			},
		},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	require.NoError(t, err)
	tok, err := josejwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)
	return tok
}

type jwksServer struct {
	*httptest.Server
	calls atomic.Int32
	keys  atomic.Pointer[jose.JSONWebKeySet]
	fail  atomic.Bool
}

func newJWKSServer(keys ...jose.JSONWebKey) *jwksServer {
	j := &jwksServer{}
	set := jose.JSONWebKeySet{Keys: keys}
	j.keys.Store(&set)
	j.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		j.calls.Add(1)
		if j.fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(j.keys.Load())
	}))
	return j
}

func (j *jwksServer) setKeys(keys ...jose.JSONWebKey) {
	set := jose.JSONWebKeySet{Keys: keys}
	j.keys.Store(&set)
}

func baseClaims(iss, aud, sub string) map[string]any {
	now := time.Now()
	return map[string]any{
		"iss": iss,
		"aud": aud,
		"sub": sub,
		"iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
	}
}

func TestVerifier_Verify_Success(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), srv.calls.Load(), "startup fetch")

	tok := s.sign(t, baseClaims("https://idp.example.com", "lakefs", "user-42"))
	claims, err := v.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.Equal(t, "user-42", claims.Subject)
	require.Equal(t, "user-42", claims.Raw["sub"])
}

// TestVerifier_Verify_RejectsStandardClaims covers the three
// go-jose claim validations we rely on: iss, aud, and exp. The test
// asserts the translation layer (*jwt.Expected mismatch ->
// ErrInvalidToken), not the library's own validation logic.
func TestVerifier_Verify_RejectsStandardClaims(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	cases := map[string]func(map[string]any){
		"wrong issuer":   func(c map[string]any) { c["iss"] = "https://rogue.example.com" },
		"wrong audience": func(c map[string]any) { c["aud"] = "other" },
		"expired": func(c map[string]any) {
			c["exp"] = time.Now().Add(-10 * time.Minute).Unix()
			c["iat"] = time.Now().Add(-20 * time.Minute).Unix()
		},
		"not yet valid (nbf in the future)": func(c map[string]any) {
			// Well beyond the 60s clock-skew leeway.
			c["nbf"] = time.Now().Add(10 * time.Minute).Unix()
		},
		"audience list with no match": func(c map[string]any) {
			c["aud"] = []string{"other", "another"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			claims := baseClaims("https://idp.example.com", "lakefs", "u")
			mutate(claims)
			_, err := v.Verify(context.Background(), s.sign(t, claims))
			require.ErrorIs(t, err, jwtidp.ErrInvalidToken)
		})
	}
}

func TestVerifier_Verify_RejectsHMAC(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	hmacSigner, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.HS256, Key: []byte("01020304050607080910111213141516")},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	require.NoError(t, err)
	tok, err := josejwt.Signed(hmacSigner).Claims(baseClaims("https://idp.example.com", "lakefs", "u")).Serialize()
	require.NoError(t, err)

	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, jwtidp.ErrInvalidToken)
}

func TestVerifier_Verify_RequiredClaimEnforced(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:                "https://idp.example.com",
		Audience:              []string{"lakefs"},
		JWKSURL:               srv.URL,
		ValidateIDTokenClaims: map[string]string{"hd": "example.com"},
	})
	require.NoError(t, err)

	claims := baseClaims("https://idp.example.com", "lakefs", "u")
	tok := s.sign(t, claims)
	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, jwtidp.ErrClaimMissing)

	claims["hd"] = "evil.com"
	tok = s.sign(t, claims)
	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, jwtidp.ErrClaimMismatch)

	claims["hd"] = "example.com"
	tok = s.sign(t, claims)
	_, err = v.Verify(context.Background(), tok)
	require.NoError(t, err)
}

func TestVerifier_Verify_UnknownKIDTriggersRefresh(t *testing.T) {
	s1 := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s1.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), srv.calls.Load())

	s2 := newSigningSet(t, "kid-2")
	srv.setKeys(s1.pub, s2.pub)

	tok := s2.sign(t, baseClaims("https://idp.example.com", "lakefs", "u"))
	claims, err := v.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.Equal(t, "u", claims.Subject)
	require.Equal(t, int32(2), srv.calls.Load(), "kid miss should trigger a single refresh")
}

func TestVerifier_Verify_RefreshFailurePreservesKnownKeys(t *testing.T) {
	s1 := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s1.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	srv.fail.Store(true)
	unknown := newSigningSet(t, "kid-missing")
	tok := unknown.sign(t, baseClaims("https://idp.example.com", "lakefs", "u"))
	_, err = v.Verify(context.Background(), tok)
	// keyFor returns ErrJWKSFetch on refresh failure deterministically;
	// the ErrUnknownKID branch only fires when refresh succeeds but
	// the fresh set still lacks the kid.
	require.ErrorIs(t, err, jwtidp.ErrJWKSFetch)

	srv.fail.Store(false)
	tok = s1.sign(t, baseClaims("https://idp.example.com", "lakefs", "u"))
	_, err = v.Verify(context.Background(), tok)
	require.NoError(t, err, "known key must still work after a failed refresh")
}

// TestVerifier_Verify_RejectsTamperedSignature is the core negative
// test for the verifier: an attacker presents a token signed by a
// private key they control but advertises the same `kid` as a key
// published by the IdP. The verifier must fetch the IdP's public key
// for that kid, fail signature verification against it, and surface
// ErrInvalidToken. A regression here (e.g. passing the token's
// embedded key instead of the JWKS-resolved one to Claims) would let
// any caller forge tokens.
func TestVerifier_Verify_RejectsTamperedSignature(t *testing.T) {
	victim := newSigningSet(t, "kid-1")
	attacker := newSigningSet(t, "kid-1")
	srv := newJWKSServer(victim.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	tok := attacker.sign(t, baseClaims("https://idp.example.com", "lakefs", "u"))
	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, jwtidp.ErrInvalidToken)
}

// TestVerifier_Verify_RejectsMissingKID ensures we refuse tokens
// without a kid header; without a kid we cannot pin verification to a
// specific published key.
func TestVerifier_Verify_RejectsMissingKID(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	// Sign without a KeyID so no `kid` header is emitted.
	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.RS256,
			Key:       jose.JSONWebKey{Key: s.priv, Algorithm: string(jose.RS256)},
		},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	require.NoError(t, err)
	tok, err := josejwt.Signed(signer).Claims(baseClaims("https://idp.example.com", "lakefs", "u")).Serialize()
	require.NoError(t, err)

	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, jwtidp.ErrMissingKID)
}

// TestVerifier_Verify_AcceptsMultiAudienceToken covers the RFC 7519
// shape where `aud` is an array. go-jose's AnyAudience requires at
// least one expected audience to appear in the token's audience list.
func TestVerifier_Verify_AcceptsMultiAudienceToken(t *testing.T) {
	s := newSigningSet(t, "kid-1")
	srv := newJWKSServer(s.pub)
	defer srv.Close()

	v, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	c := baseClaims("https://idp.example.com", "lakefs", "u")
	c["aud"] = []string{"other-service", "lakefs"}
	_, err = v.Verify(context.Background(), s.sign(t, c))
	require.NoError(t, err)
}

func TestVerifier_New_FailsWhenJWKSUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := jwtidp.New(&config.JWT{
		Issuer:   "https://idp.example.com",
		Audience: []string{"lakefs"},
		JWKSURL:  srv.URL,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, jwtidp.ErrJWKSFetch)
}

func TestVerifier_New_ValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.JWT
	}{
		{"nil", nil},
		{"missing issuer", &config.JWT{Audience: []string{"a"}, JWKSURL: "http://x"}},
		{"missing audience", &config.JWT{Issuer: "i", JWKSURL: "http://x"}},
		{"missing jwks_url", &config.JWT{Issuer: "i", Audience: []string{"a"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := jwtidp.New(tc.cfg)
			require.Error(t, err)
			require.ErrorIs(t, err, jwtidp.ErrConfig)
		})
	}
}
