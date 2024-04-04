package sig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/logging"
)

// Implements the "signing protocol" as exists at aws-sdk-java @ 1.12.390 (commit 07926f08a70).
// This should never have worked, on any version of S3, but the implementation is there, unmodified since 2015.
// The code is below, for reference
/*

private String calculateStringToSignV2(SignableRequest<?> request) throws SdkClientException {
   URI endpoint = request.getEndpoint();

   StringBuilder data = new StringBuilder();
   data.append("POST")
	   .append("\n")
	   .append(getCanonicalizedEndpoint(endpoint)) // <----- bare host, lower-cased
	   .append("\n")
	   .append(getCanonicalizedResourcePath(request)) // <----- path relative to the endpoint
	   .append("\n")
	   .append(getCanonicalizedQueryString(request.getParameters()));  // <----- ordered query string parameters
   return data.toString();
}

*/

func canonicalJavaV2String(host string, query url.Values, path string) string {
	cs := strings.ToUpper(http.MethodPost) // so weird.
	cs += "\n"
	cs += host
	cs += "\n"
	cs += path
	cs += "\n"
	cs += canonicalJavaV2Query(query)
	return cs
}

func canonicalJavaV2Query(q url.Values) string {
	escaped := make([][2]string, 0)
	for k, vs := range q {
		if strings.EqualFold(k, "signature") {
			continue
		}
		escapedKey := url.QueryEscape(k)
		for _, v := range vs {
			pair := [2]string{escapedKey, url.QueryEscape(v)}
			escaped = append(escaped, pair)
		}
	}
	// sort
	sort.Slice(escaped, func(i, j int) bool {
		return escaped[i][0] < escaped[j][0]
	})
	// output
	out := ""
	for i, pair := range escaped {
		out += pair[0] + "=" + pair[1]
		isLast := i == len(escaped)-1
		if !isLast {
			out += "&"
		}
	}
	return out
}

type JavaV2Signer struct {
	bareDomain string
	req        *http.Request
	sigCtx     *JavaV2SignerContext
}

type JavaV2SignerContext struct {
	awsAccessKeyID string
	signature      []byte
}

func NewJavaV2SigAuthenticator(r *http.Request, bareDomain string) *JavaV2Signer {
	return &JavaV2Signer{
		req:        r,
		bareDomain: bareDomain,
	}
}

func (j *JavaV2SignerContext) GetAccessKeyID() string {
	return j.awsAccessKeyID
}

func (j *JavaV2Signer) Parse() (SigContext, error) {
	ctx := j.req.Context()
	awsAccessKeyID := j.req.URL.Query().Get("AWSAccessKeyId")
	if awsAccessKeyID == "" {
		return nil, ErrHeaderMalformed
	}
	signature := j.req.URL.Query().Get("Signature")
	if signature == "" {
		return nil, ErrHeaderMalformed
	}
	sig, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		logging.FromContext(ctx).Error("log header does not match v2 structure (isn't proper base64)")
		return nil, ErrHeaderMalformed
	}
	sigMethod := j.req.URL.Query().Get("SignatureMethod")
	if sigMethod != "HmacSHA256" {
		return nil, ErrHeaderMalformed
	}
	sigVersion := j.req.URL.Query().Get("SignatureVersion")
	if sigVersion != "2" {
		return nil, ErrHeaderMalformed
	}
	sigCtx := &JavaV2SignerContext{
		awsAccessKeyID: awsAccessKeyID,
		signature:      sig,
	}
	j.sigCtx = sigCtx
	return sigCtx, nil
}

func signCanonicalJavaV2String(msg string, signature []byte) []byte {
	h := hmac.New(sha256.New, signature)
	_, _ = h.Write([]byte(msg))
	return h.Sum(nil)
}

func (j *JavaV2Signer) Verify(creds *model.Credential) error {
	rawPath := j.req.URL.EscapedPath()

	path := buildPath(j.req.Host, j.bareDomain, rawPath)
	stringToSign := canonicalJavaV2String(j.req.Host, j.req.URL.Query(), path)
	digest := signCanonicalJavaV2String(stringToSign, []byte(creds.SecretAccessKey))
	if !Equal(digest, j.sigCtx.signature) {
		return errors.ErrSignatureDoesNotMatch
	}
	return nil
}
