package lua

import (
	"context"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/crypto/aes"
	"github.com/treeverse/lakefs/pkg/actions/lua/crypto/hmac"
	"github.com/treeverse/lakefs/pkg/actions/lua/crypto/sha256"
	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/base64"
	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/hex"
	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/json"
	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/parquet"
	"github.com/treeverse/lakefs/pkg/actions/lua/net/http"
	"github.com/treeverse/lakefs/pkg/actions/lua/path"
	"github.com/treeverse/lakefs/pkg/actions/lua/regexp"
	"github.com/treeverse/lakefs/pkg/actions/lua/storage/aws"
	"github.com/treeverse/lakefs/pkg/actions/lua/strings"
	"github.com/treeverse/lakefs/pkg/actions/lua/time"
	"github.com/treeverse/lakefs/pkg/actions/lua/uuid"
)

// most classes here are taken from: https://github.com/Shopify/goluago
// See the original MIT license with copyright at ./LICENSE.md

func Open(l *lua.State, ctx context.Context, cfg OpenSafeConfig) {
	regexp.Open(l)
	strings.Open(l)
	json.Open(l)
	time.Open(l)
	hmac.Open(l)
	base64.Open(l)
	uuid.Open(l)
	hex.Open(l)
	sha256.Open(l)
	aes.Open(l)
	parquet.Open(l)
	path.Open(l)
	aws.Open(l, ctx)
	if cfg.NetHTTPEnabled {
		http.Open(l)
	}
}
