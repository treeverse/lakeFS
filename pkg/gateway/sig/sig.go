package sig

import (
	"crypto/hmac"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	gwErrors "github.com/treeverse/lakefs/pkg/gateway/errors"

	"github.com/treeverse/lakefs/pkg/auth/model"

	"errors"
)

var (
	ErrHeaderMalformed = errors.New("header malformed")

	// if object matches reserved string, no need to encode them
	reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
)

// taken from https://github.com/minio/minio-go/blob/master/pkg/s3utils/utils.go
/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// EncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func EncodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname += string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname += string(s)
			continue
		default:
			runeLen := utf8.RuneLen(s)
			if runeLen < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, runeLen)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				h := hex.EncodeToString([]byte{r})
				encodedPathname += "%" + strings.ToUpper(h)
			}
		}
	}
	return encodedPathname
}

type SigContext interface {
	GetAccessKeyID() string
}

type SigAuthenticator interface {
	Parse() (SigContext, error)
	Verify(*model.Credential, string) error
}

type chainedAuthenticator struct {
	methods []SigAuthenticator
	chosen  SigAuthenticator
}

func ChainedAuthenticator(methods ...SigAuthenticator) SigAuthenticator {
	return &chainedAuthenticator{methods, nil}
}

func (c *chainedAuthenticator) Parse() (SigContext, error) {
	for _, method := range c.methods {
		ctx, err := method.Parse()
		if err == nil {
			c.chosen = method
			return ctx, nil
		}
	}
	return nil, gwErrors.ErrMissingFields
}

func Equal(sig1, sig2 []byte) bool {
	return hmac.Equal(sig1, sig2)
}

func (c *chainedAuthenticator) Verify(creds *model.Credential, domain string) error {
	return c.chosen.Verify(creds, domain)
}

func (c *chainedAuthenticator) String() string {
	if c.chosen == nil {
		return "chained authenticator"
	}
	return fmt.Sprintf("%s", c.chosen)
}
