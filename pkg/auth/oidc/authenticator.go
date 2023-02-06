package oidc

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/auth/oidc/encoding"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

var ErrTokenExtract = errors.New("failed to extract id token")

// Claims is encoding.Claims.  To preserve serialization, it must be an
// alias declaration of encoding.Claims (with an "=") and not a type
// declaration.
type Claims = encoding.Claims

type Authenticator struct {
	oauthConfig  *oauth2.Config
	oidcProvider *oidc.Provider
}

func NewAuthenticator(oauthConfig *oauth2.Config, oidcProvider *oidc.Provider) *Authenticator {
	return &Authenticator{oauthConfig: oauthConfig, oidcProvider: oidcProvider}
}

// GetIDTokenClaims exchanges a temporary code for an ID token.
// The ID token is verified to be valid, and its Claims are then returned.
func (a *Authenticator) GetIDTokenClaims(ctx context.Context, code string) (Claims, error) {
	token, err := a.oauthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, err
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return nil, ErrTokenExtract
	}
	oidcVerifier := a.oidcProvider.Verifier(&oidc.Config{
		ClientID: a.oauthConfig.ClientID,
	})
	idToken, err := oidcVerifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, err
	}
	var claims Claims
	if err := idToken.Claims(&claims); err != nil {
		return nil, err
	}
	return claims, nil
}
