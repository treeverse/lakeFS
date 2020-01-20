package sig

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

const (
	v2authHeaderName = "Authorization"
)

var (
	V2AuthHeaderRegexp = regexp.MustCompile(`AWS (?P<AccessKeyId>[A-Z0-9]{20}):(?P<Signature>[A-Za-z0-9+/=]+)`)
)

type v2Context struct {
	accessKeyId string
	signature   []byte
}

func (a v2Context) GetAccessKeyId() string {
	return a.accessKeyId
}

type V2SigAuthenticator struct {
	r   *http.Request
	ctx v2Context
}

func NewV2SigAuthenticator(r *http.Request) *V2SigAuthenticator {
	return &V2SigAuthenticator{
		r: r,
	}
}

func (a *V2SigAuthenticator) Parse() (SigContext, error) {
	var ctx v2Context
	headerValue := a.r.Header.Get(v2authHeaderName)
	if len(headerValue) > 0 {
		match := V2AuthHeaderRegexp.FindStringSubmatch(headerValue)
		if len(match) == 0 {
			log.WithField("header", headerValue).Error("log header does not match v2 structure")
			return ctx, ErrHeaderMalformed
		}
		result := make(map[string]string)
		for i, name := range V2AuthHeaderRegexp.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
		ctx.accessKeyId = result["AccessKeyId"]
		// parse signature
		sig, err := base64.StdEncoding.DecodeString(result["Signature"])
		if err != nil {
			log.WithField("header", headerValue).Error("log header does not match v2 structure (isn't proper base64)")
			return ctx, ErrHeaderMalformed
		}
		ctx.signature = sig
	}
	a.ctx = ctx
	return ctx, nil
}

func header_value_to_string(val []string) (return_str string) {
	for i, item := range val {
		if i == 0 {
			return_str = strings.TrimSpace(item)
		} else {
			return_str += "," + strings.TrimSpace(item)
		}
	}
	return
}

func canonical_standard_headers(headers http.Header) (return_str string) {
	var interesting_headers = [...]string{"content-md5", "content-type", "date"}
	for _, hoi := range interesting_headers {
		foundHoi := false
		for key, val := range headers {
			if len(val) > 0 && strings.ToLower(key) == hoi {
				return_str += header_value_to_string(val) + "\n"
				foundHoi = true
				break
			}

		}
		if !foundHoi {
			return_str += "\n"
		}
	}
	return
}

func canonical_custom_headers(headers http.Header) (return_str string) {
	var foundKeys []string
	for key, _ := range headers {
		lk := strings.ToLower(key)
		if strings.HasPrefix(lk, "x-amz-") {
			foundKeys = append(foundKeys, lk)
		}
	}
	if len(foundKeys) == 0 {
		return
	}
	sort.Strings(foundKeys)
	for _, key := range foundKeys {
		return_str += fmt.Sprint(key, ":", header_value_to_string(headers[key]), "\n")
	}
	return
}

func canonical_string(method /*query,expires,authPath*/ string, headers http.Header) string {
	cs := strings.ToUpper(method) + "\n"
	cs += canonical_standard_headers(headers)
	cs += canonical_custom_headers(headers)
	// todo:custom resources
	return cs
}

func signCanonicalString(msg string, signature []byte) (digest []byte) {
	h := hmac.New(sha1.New, []byte(signature))
	h.Write([]byte(msg))
	digest = h.Sum(nil)
	return
}
func (a *V2SigAuthenticator) Verify(creds Credentials) error {
	/*
		s3 sigV2 implementation:
		the s3 signature  is somewhat different than general aws signature implementation.
		in boto3 configuration their value is 's3' and 's3v4' respectively, while the general aws signatures are
		'v2' and 'v4'.
		in 2020, the GO aws sdk does not inplement 's3' signature, So i will "translate" it from boto3.
		source is class botocore.auth.HmacV1Auth
		steps in building the string to be signed:
		1. create initial string, with uppercase http method + '\n'
		2. collect all required headers:
			standard headers - 'content-md5', 'content-type', 'date' - if one of those does not appear, it is replaces with an
			empty line '\n'. sorted and stringified
			custom headers - any header that starts with 'x-amz-'. if the header appears more than once - the values
			are joined with ',' seperator. sorted and stringified.
			QSA(Query String Arguments) - todo: continue explanation
	*/
	stringToSigh := canonical_string(a.r.Method, a.r.Header)
	stringToSigh += a.r.URL.Path // todo: hack - move to implementation of custom resources
	digest := signCanonicalString(stringToSigh, []byte(creds.GetAccessSecretKey()))
	if !hmac.Equal(digest, a.ctx.signature) {
		return ErrBadSignature
	}
	return nil
}

func (a *V2SigAuthenticator) String() string {
	return "sigv2"
}
