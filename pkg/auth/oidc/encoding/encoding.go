// Package encoding defines Claims for interoperable external services to
// use in JWTs.  An external service that imports this package receives a
// Claims with a stable gob encoding.
package encoding

import "encoding/gob"

type Claims map[string]interface{}

// OIDCClaimsSerdeNickname is the typename used to serialize Claims using
// gob encoding in JWT.  It is the default value that gob would give had
// Claims been part of auth.  It is not (any longer), explicitly to allow
// external services to serialize matching claims.
const OIDCClaimsSerdeNickname = "github.com/treeverse/lakefs/pkg/auth/oidc.Claims"

// init registers OIDCSerdeNickname as the typename used to serialize Claims
// using gob encoding in JWT.  It should be called in an init() func of a
// package in any external service that produces matching claims.
func init() {
	gob.RegisterName(OIDCClaimsSerdeNickname, Claims{})
}
