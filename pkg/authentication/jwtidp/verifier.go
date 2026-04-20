// Package jwtidp verifies JWTs issued by an external identity provider
// and maps their claims onto lakeFS users. It backs the
// POST /auth/jwt/login token-exchange endpoint.
package jwtidp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"
	"github.com/treeverse/lakefs/pkg/config"
)

const (
	jwksHTTPTimeout = 10 * time.Second
	clockSkew       = 60 * time.Second
)

var (
	ErrConfig        = errors.New("jwtidp: invalid configuration")
	ErrJWKSFetch     = errors.New("jwtidp: fetch jwks")
	ErrUnknownKID    = errors.New("jwtidp: unknown kid")
	ErrMissingKID    = errors.New("jwtidp: missing kid header")
	ErrClaimMismatch = errors.New("jwtidp: required claim mismatch")
	ErrClaimMissing  = errors.New("jwtidp: required claim missing")
	ErrInvalidToken  = errors.New("jwtidp: invalid token")
)

// supportedAlgs is the full set of asymmetric JWS algorithms accepted by
// the verifier. HMAC (HS*) is deliberately excluded: an IdP JWT must be
// verifiable using a published JWK, never shared symmetric material.
var supportedAlgs = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.PS256, jose.PS384, jose.PS512,
}

// Claims is a verified JWT claim set. Raw holds the full decoded map
// for JSON-pointer extraction; the other fields are the typed
// spec-defined claims already validated during Verify.
type Claims struct {
	Raw      map[string]any
	Subject  string
	Expiry   time.Time
	IssuedAt time.Time
}

// Verifier verifies JWTs issued by a single external IdP. It is safe
// for concurrent use. The cached JWKS is refreshed on-demand when a
// token presents an unknown `kid`; a failed refresh leaves the
// previously known keys in place.
type Verifier struct {
	expected josejwt.Expected
	required map[string]string
	algs     []jose.SignatureAlgorithm

	jwksURL string
	http    *http.Client
	keys    atomic.Pointer[jose.JSONWebKeySet]
}

// New builds a Verifier and primes the JWKS cache. Fails fast if cfg
// is incomplete or the JWKS endpoint is unreachable at startup.
func New(cfg *config.JWT) (*Verifier, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil", ErrConfig)
	}
	if cfg.Issuer == "" {
		return nil, fmt.Errorf("%w: issuer is required", ErrConfig)
	}
	if len(cfg.Audience) == 0 {
		return nil, fmt.Errorf("%w: audience is required", ErrConfig)
	}
	if cfg.JWKSURL == "" {
		return nil, fmt.Errorf("%w: jwks_url is required", ErrConfig)
	}

	required := make(map[string]string, len(cfg.ValidateIDTokenClaims))
	for k, v := range cfg.ValidateIDTokenClaims {
		required[k] = v
	}

	v := &Verifier{
		expected: josejwt.Expected{
			Issuer:      cfg.Issuer,
			AnyAudience: josejwt.Audience(cfg.Audience),
		},
		required: required,
		algs:     supportedAlgs,
		jwksURL:  cfg.JWKSURL,
		http:     &http.Client{Timeout: jwksHTTPTimeout},
	}

	ctx, cancel := context.WithTimeout(context.Background(), jwksHTTPTimeout)
	defer cancel()
	if _, err := v.refresh(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrJWKSFetch, err)
	}
	return v, nil
}

// Verify parses and validates a raw JWT against the configured
// issuer, audience, required claims, and cached JWKS.
func (v *Verifier) Verify(ctx context.Context, raw string) (Claims, error) {
	token, err := josejwt.ParseSigned(raw, v.algs)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: parse: %w", ErrInvalidToken, err)
	}
	if len(token.Headers) == 0 {
		return Claims{}, fmt.Errorf("%w: no headers", ErrInvalidToken)
	}
	kid := token.Headers[0].KeyID
	if kid == "" {
		return Claims{}, ErrMissingKID
	}

	key, err := v.keyFor(ctx, kid)
	if err != nil {
		return Claims{}, err
	}

	var (
		std       josejwt.Claims
		rawClaims map[string]any
	)
	if err := token.Claims(key.Key, &std, &rawClaims); err != nil {
		return Claims{}, fmt.Errorf("%w: verify: %w", ErrInvalidToken, err)
	}
	if err := std.ValidateWithLeeway(v.expected.WithTime(time.Now()), clockSkew); err != nil {
		return Claims{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	for name, want := range v.required {
		got, ok := rawClaims[name]
		if !ok {
			return Claims{}, fmt.Errorf("%w: %q", ErrClaimMissing, name)
		}
		gotStr, ok := got.(string)
		if !ok || gotStr != want {
			return Claims{}, fmt.Errorf("%w: %q", ErrClaimMismatch, name)
		}
	}

	out := Claims{
		Raw:     rawClaims,
		Subject: std.Subject,
	}
	if std.Expiry != nil {
		out.Expiry = std.Expiry.Time()
	}
	if std.IssuedAt != nil {
		out.IssuedAt = std.IssuedAt.Time()
	}
	return out, nil
}

func (v *Verifier) keyFor(ctx context.Context, kid string) (*jose.JSONWebKey, error) {
	if k := lookupKey(v.keys.Load(), kid); k != nil {
		return k, nil
	}
	set, err := v.refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrJWKSFetch, err)
	}
	if k := lookupKey(set, kid); k != nil {
		return k, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKID, kid)
}

func lookupKey(set *jose.JSONWebKeySet, kid string) *jose.JSONWebKey {
	if set == nil {
		return nil
	}
	matches := set.Key(kid)
	if len(matches) == 0 {
		return nil
	}
	return &matches[0]
}

// refresh fetches the JWKS, atomically publishes it as the current
// cache, and returns it so callers on the kid-miss path can look up
// the fresh set without re-Load()-ing the atomic pointer. A failed
// fetch leaves the previously cached set in place.
func (v *Verifier) refresh(ctx context.Context) (*jose.JSONWebKeySet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %s", ErrJWKSFetch, resp.Status)
	}
	var set jose.JSONWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	v.keys.Store(&set)
	return &set, nil
}
