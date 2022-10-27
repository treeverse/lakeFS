package sig

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
)

const (
	V4authHeaderName        = "Authorization"
	V4authHeaderPrefix      = "AWS4-HMAC-SHA256"
	AmzDecodedContentLength = "X-Amz-Decoded-Content-Length"
	v4StreamingPayloadHash  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	v4UnsignedPayload       = "UNSIGNED-PAYLOAD"
	v4authHeaderPayload     = "x-amz-content-sha256"
	v4scopeTerminator       = "aws4_request"
	v4timeFormat            = "20060102T150405Z"
	v4shortTimeFormat       = "20060102"
	v4SignatureHeader       = "X-Amz-Signature"
)

var (
	V4AuthHeaderRegexp      = regexp.MustCompile(`AWS4-HMAC-SHA256 Credential=(?P<AccessKeyId>.{3,20})/(?P<Date>\d{8})/(?P<Region>[\w\-]+)/(?P<Service>[\w\-]+)/aws4_request,\s*SignedHeaders=(?P<SignatureHeaders>[\w\-\;]+),\s*Signature=(?P<Signature>[abcdef0123456789]{64})`)
	V4CredentialScopeRegexp = regexp.MustCompile(`(?P<AccessKeyId>.{3,20})/(?P<Date>\d{8})/(?P<Region>[\w\-]+)/(?P<Service>[\w\-]+)/aws4_request`)
)

type V4Auth struct {
	AccessKeyID         string
	Date                string
	Region              string
	Service             string
	SignedHeaders       []string
	SignedHeadersString string
	Signature           string
}

func (a V4Auth) GetAccessKeyID() string {
	return a.AccessKeyID
}

func splitHeaders(headers string) []string {
	headerValues := strings.Split(headers, ";")
	sort.Strings(headerValues)
	return headerValues
}

func ParseV4AuthContext(r *http.Request) (V4Auth, error) {
	var ctx V4Auth

	// start by trying to extract the data from the Authorization header
	headerValue := r.Header.Get(V4authHeaderName)
	if len(headerValue) > 0 {
		match := V4AuthHeaderRegexp.FindStringSubmatch(headerValue)
		if len(match) == 0 {
			return ctx, ErrHeaderMalformed
		}
		result := make(map[string]string)
		for i, name := range V4AuthHeaderRegexp.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
		headers := splitHeaders(result["SignatureHeaders"])
		ctx.AccessKeyID = result["AccessKeyId"]
		ctx.Date = result["Date"]
		ctx.Region = result["Region"]
		ctx.Service = result["Service"]

		ctx.Signature = result["Signature"]

		ctx.SignedHeaders = headers
		ctx.SignedHeadersString = result["SignatureHeaders"]
		return ctx, nil
	}

	// otherwise, see if we have all the required query parameters
	query := r.URL.Query()
	algorithm := query.Get("X-Amz-Algorithm")
	if len(algorithm) == 0 || !strings.EqualFold(algorithm, V4authHeaderPrefix) {
		return ctx, errors.ErrInvalidQuerySignatureAlgo
	}
	credentialScope := query.Get("X-Amz-Credential")
	if len(credentialScope) == 0 {
		return ctx, errors.ErrMissingCredTag
	}
	credsMatch := V4CredentialScopeRegexp.FindStringSubmatch(credentialScope)
	if len(credsMatch) == 0 {
		return ctx, errors.ErrCredMalformed
	}
	credsResult := make(map[string]string)
	for i, name := range V4CredentialScopeRegexp.SubexpNames() {
		if i != 0 && name != "" {
			credsResult[name] = credsMatch[i]
		}
	}
	ctx.AccessKeyID = credsResult["AccessKeyId"]
	ctx.Date = credsResult["Date"]
	ctx.Region = credsResult["Region"]
	ctx.Service = credsResult["Service"]

	ctx.SignedHeadersString = query.Get("X-Amz-SignedHeaders")
	headers := splitHeaders(ctx.SignedHeadersString)
	ctx.SignedHeaders = headers
	ctx.Signature = query.Get(v4SignatureHeader)
	return ctx, nil
}

func V4Verify(auth V4Auth, credentials *model.Credential, r *http.Request) error {
	ctx := &verificationCtx{
		Request:   r,
		Query:     r.URL.Query(),
		AuthValue: auth,
	}

	canonicalRequest := ctx.buildCanonicalRequest()
	stringToSign, err := ctx.buildSignedString(canonicalRequest)
	if err != nil {
		return err
	}
	// sign
	signingKey := createSignature(credentials.SecretAccessKey, auth.Date, auth.Region, auth.Service)
	signature := hex.EncodeToString(sign(signingKey, stringToSign))

	// compare signatures
	if !Equal([]byte(signature), []byte(auth.Signature)) {
		return errors.ErrSignatureDoesNotMatch
	}

	// wrap body with verifier
	reader, err := ctx.reader(r.Body, credentials)
	if err != nil {
		return err
	}
	r.Body = reader

	// update to decoded content length
	contentLength, err := ctx.contentLength()
	if err != nil {
		return err
	}
	r.ContentLength = contentLength
	return nil
}

type verificationCtx struct {
	Request   *http.Request
	Query     url.Values
	AuthValue V4Auth
}

func (ctx *verificationCtx) queryEscape(str string) string {
	return strings.ReplaceAll(url.QueryEscape(str), "+", "%20")
}

func (ctx *verificationCtx) canonicalizeQueryString() string {
	queryNames := make([]string, 0, len(ctx.Query))
	for k := range ctx.Query {
		if k == v4SignatureHeader {
			continue
		}
		queryNames = append(queryNames, k)
	}
	sort.Strings(queryNames)
	buf := make([]string, len(queryNames))
	for i, key := range queryNames {
		buf[i] = fmt.Sprintf("%s=%s", ctx.queryEscape(key), ctx.queryEscape(ctx.Query.Get(key)))
	}
	return strings.Join(buf, "&")
}

func (ctx *verificationCtx) canonicalizeHeaders(headers []string) string {
	var buf strings.Builder
	for _, header := range headers {
		var value string
		if strings.EqualFold(strings.ToLower(header), "host") {
			// in Go, Host is removed from the headers and is promoted to request.Host for some reason
			value = ctx.Request.Host
		} else {
			value = getInsensitiveHeader(ctx.Request, header)
		}
		buf.WriteString(header)
		buf.WriteString(":")
		buf.WriteString(ctx.trimAll(value))
		buf.WriteString("\n")
	}
	return buf.String()
}

func (ctx *verificationCtx) trimAll(str string) string {
	str = strings.TrimSpace(str)
	inSpace := false
	var buf strings.Builder
	for _, ch := range str {
		if unicode.IsSpace(ch) {
			if !inSpace {
				// first space to appear
				buf.WriteRune(ch)
				inSpace = true
			}
		} else {
			// not a space
			buf.WriteRune(ch)
			inSpace = false
		}
	}
	return buf.String()
}

func getInsensitiveHeader(r *http.Request, headerName string) string {
	for k, v := range r.Header {
		if strings.EqualFold(k, headerName) {
			return v[0]
		}
	}
	return ""
}

func (ctx *verificationCtx) payloadHash() string {
	payloadHash := getInsensitiveHeader(ctx.Request, v4authHeaderPayload)
	if payloadHash == "" {
		return v4UnsignedPayload
	}
	return payloadHash
}

func (ctx *verificationCtx) buildCanonicalRequest() string {
	// Step 1: Canonical request
	method := ctx.Request.Method
	canonicalURI := EncodePath(ctx.Request.URL.Path)
	canonicalQueryString := ctx.canonicalizeQueryString()
	canonicalHeaders := ctx.canonicalizeHeaders(ctx.AuthValue.SignedHeaders)
	signedHeaders := ctx.AuthValue.SignedHeadersString
	payloadHash := ctx.payloadHash()
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	return canonicalRequest
}

func (ctx *verificationCtx) getAmzDate() (string, error) {
	// https://docs.aws.amazon.com/general/latest/gr/sigv4-date-handling.html
	amzDate := ctx.Request.URL.Query().Get("X-Amz-Date")
	if len(amzDate) == 0 {
		amzDate = ctx.Request.Header.Get("x-amz-date")
		if len(amzDate) == 0 {
			amzDate = ctx.Request.Header.Get("date")
			if len(amzDate) == 0 {
				return "", errors.ErrMissingDateHeader
			}
		}
	}

	// parse date
	ts, err := time.Parse(v4timeFormat, amzDate)
	if err != nil {
		return "", errors.ErrMalformedDate
	}

	// parse signature date
	sigTS, err := time.Parse(v4shortTimeFormat, ctx.AuthValue.Date)
	if err != nil {
		return "", errors.ErrMalformedCredentialDate
	}

	// ensure same date
	if sigTS.Year() != ts.Year() || sigTS.Month() != ts.Month() || sigTS.Day() != ts.Day() {
		return "", errors.ErrMalformedCredentialDate
	}

	return amzDate, nil
}

func sign(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(msg))
	return h.Sum(nil)
}

func createSignature(key, dateStamp, region, service string) []byte {
	kDate := sign([]byte(fmt.Sprintf("AWS4%s", key)), dateStamp)
	kRegion := sign(kDate, region)
	kService := sign(kRegion, service)
	kSigning := sign(kService, v4scopeTerminator)
	return kSigning
}

func (ctx *verificationCtx) buildSignedString(canonicalRequest string) (string, error) {
	// Step 2: Create string to sign
	algorithm := V4authHeaderPrefix
	credentialScope := strings.Join([]string{
		ctx.AuthValue.Date,
		ctx.AuthValue.Region,
		ctx.AuthValue.Service,
		v4scopeTerminator,
	}, "/")
	amzDate, err := ctx.getAmzDate()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := h.Write([]byte(canonicalRequest)); err != nil {
		return "", err
	}
	hashedCanonicalRequest := hex.EncodeToString(h.Sum(nil))
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		credentialScope,
		hashedCanonicalRequest,
	}, "\n")
	return stringToSign, nil
}

func (ctx *verificationCtx) isStreaming() bool {
	payloadHash := ctx.payloadHash()
	return strings.EqualFold(payloadHash, v4StreamingPayloadHash)
}

func (ctx *verificationCtx) isUnsigned() bool {
	return strings.EqualFold(ctx.payloadHash(), v4UnsignedPayload)
}

func (ctx *verificationCtx) contentLength() (int64, error) {
	size := ctx.Request.ContentLength
	if ctx.isStreaming() {
		if sizeStr, ok := ctx.Request.Header[AmzDecodedContentLength]; ok {
			if sizeStr[0] == "" {
				return 0, errors.ErrMissingContentLength
			}
			var err error
			size, err = strconv.ParseInt(sizeStr[0], 10, 64) //nolint: gomnd
			if err != nil {
				return 0, err
			}
		}
	}
	return size, nil
}

func (ctx *verificationCtx) reader(reader io.ReadCloser, creds *model.Credential) (io.ReadCloser, error) {
	if ctx.isStreaming() {
		amzDate, err := ctx.getAmzDate()
		if err != nil {
			return nil, err
		}
		chunkReader, err := newSignV4ChunkedReader(bufio.NewReader(reader), amzDate, ctx.AuthValue, creds)
		if err != nil {
			return nil, err
		}
		return chunkReader, nil
	}

	if ctx.isUnsigned() {
		return reader, nil
	}
	return NewSha265Reader(reader, ctx.payloadHash())
}

type V4Authenticator struct {
	request *http.Request
	ctx     V4Auth
}

func (a *V4Authenticator) Parse() (SigContext, error) {
	var ctx V4Auth
	var err error
	ctx, err = ParseV4AuthContext(a.request)
	if err != nil {
		return ctx, err
	}
	a.ctx = ctx
	return a.ctx, nil
}

func (a *V4Authenticator) String() string {
	return "sigv4"
}

func (a *V4Authenticator) Verify(creds *model.Credential, _ string) error {
	err := V4Verify(a.ctx, creds, a.request)
	return err
}

func NewV4Authenticator(r *http.Request) SigAuthenticator {
	return &V4Authenticator{
		request: r,
		ctx:     V4Auth{},
	}
}
