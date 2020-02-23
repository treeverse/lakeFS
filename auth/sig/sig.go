package sig

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/xerrors"
)

var (
	ErrHeaderMalformed        = xerrors.New("header malformed")
	ErrMissingDateHeader      = xerrors.New("missing X-Amz-Date or Date header")
	ErrDateHeaderMalformed    = xerrors.New("wrong format for date header")
	ErrSignatureDateMalformed = xerrors.New("signature date malformed")
	ErrBadSignature           = xerrors.New("bad signature")
	ErrMissingAuthData        = xerrors.New("missing authorization information")

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
			encodedPathname = encodedPathname + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}

type SigContext interface {
	GetAccessKeyId() string
}

type Credentials interface {
	GetAccessSecretKey() string
}

type SigAuthenticator interface {
	Parse() (SigContext, error)
	Verify(Credentials, string) error
}

type dummySigContext struct {
	accessKey string
}

func (d *dummySigContext) GetAccessKeyId() string {
	return d.accessKey
}

type dummyAuthenticator struct {
	ctx *dummySigContext
}

func (d dummyAuthenticator) Parse() (SigContext, error) {
	return d.ctx, nil
}

func (d dummyAuthenticator) Verify(c Credentials, s string) error {
	return nil
}

func DummyAuthenticator(accessKey string) SigAuthenticator {
	return &dummyAuthenticator{
		&dummySigContext{accessKey: accessKey},
	}
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
	return nil, ErrMissingAuthData
}

func (c *chainedAuthenticator) Verify(creds Credentials, domain string) error {
	return c.chosen.Verify(creds, domain)

}

func (c *chainedAuthenticator) String() string {
	if c.chosen == nil {
		return "chained authenticator"
	}
	return fmt.Sprintf("%s", c.chosen)
}
