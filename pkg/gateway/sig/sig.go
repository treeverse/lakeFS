package sig

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/treeverse/lakefs/pkg/auth/model"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
)

const (
	// AmzMaxClockSkew is the maximum allowed clock skew (15 minutes) for AWS S3 compatibility.
	// All signature methods (V2, JavaV2, V4) validate request timestamps are within this window
	// to prevent replay attacks.
	AmzMaxClockSkew = 15 * time.Minute
)

var (
	ErrHeaderMalformed        = errors.New("header malformed")
	ErrBadAuthorizationFormat = errors.New("authorization format not supported by this authenticator")

	// reservedObjectNames - if object matches reserved string, no need to encode them
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
	var encodedPathname strings.Builder
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname.WriteString(string(s))
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname.WriteString(string(s))
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
				encodedPathname.WriteString("%" + strings.ToUpper(h))
			}
		}
	}
	return encodedPathname.String()
}

type SigContext interface {
	GetAccessKeyID() string
}

type SigAuthenticator interface {
	Parse() (SigContext, error)
	Verify(*model.Credential) error
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
		sigContext, err := method.Parse()
		if err == nil {
			c.chosen = method
			return sigContext, nil
		} else if !errors.Is(err, ErrHeaderMalformed) && !errors.Is(err, ErrBadAuthorizationFormat) {
			// ErrHeaderMalformed and ErrBadAuthorizationFormat indicate "wrong auth format, try next method".
			// All other errors mean the request matched this method's format but failed validation,
			// so return immediately without trying remaining methods.
			return nil, err
		}
	}
	return nil, gatewayerrors.ErrMissingFields
}

func Equal(sig1, sig2 []byte) bool {
	return hmac.Equal(sig1, sig2)
}

// ValidateClockSkew ensures the request timestamp is within AmzMaxClockSkew (15 minutes)
// of the current time to prevent replay attacks. This validation is used by all AWS
// signature methods (V2, JavaV2, V4).
func ValidateClockSkew(now, requestTime time.Time) error {
	timeDiff := now.Sub(requestTime)

	if timeDiff < -AmzMaxClockSkew {
		return gatewayerrors.ErrRequestNotReadyYet
	} else if timeDiff > AmzMaxClockSkew {
		return gatewayerrors.ErrRequestTimeTooSkewed
	}

	return nil
}

func (c *chainedAuthenticator) Verify(creds *model.Credential) error {
	return c.chosen.Verify(creds)
}

func (c *chainedAuthenticator) String() string {
	if c.chosen == nil {
		return "chained authenticator"
	}
	return fmt.Sprintf("%s", c.chosen)
}

func IsAWSSignedRequest(req *http.Request) bool {
	// headers first
	headers := req.Header
	v4Value := headers.Get(v4SignatureHeader)
	if len(v4Value) > 0 {
		return true
	}
	v4AuthHeader := headers.Get(V4authHeaderName)
	if strings.HasPrefix(v4AuthHeader, "AWS4") {
		return true
	}
	v2Value := headers.Get(v2authHeaderName)
	if strings.HasPrefix(v2Value, "AWS ") {
		return true
	}

	// then request params
	queryParams := req.URL.Query()
	// sigv2: https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html
	if len(queryParams.Get("AWSAccessKeyId")) > 0 {
		return true
	}
	// sigv4: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	if len(queryParams.Get("X-Amz-Credential")) > 0 {
		return true
	}
	return false
}
