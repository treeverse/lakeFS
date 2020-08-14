package sig

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
)

const (
	v2authHeaderName = "Authorization"
)

var (
	V2AuthHeaderRegexp = regexp.MustCompile(`AWS (?P<AccessKeyId>[A-Z0-9]{20}):(?P<Signature>[A-Za-z0-9+/=]+)`)
	// Both "interesting" arrays are sorted. so once we extract relevant items by looping on them = the result is sorted
	interestingHeaders   = [...]string{"content-md5", "content-type", "date"}
	interestingResources []string // initialized and sorted by the init function

)

//nolint:gochecknoinits
func init() {
	interestingResourcesContainer := []string{
		"accelerate", "acl", "cors", "defaultObjectAcl",
		"location", "logging", "partNumber", "policy",
		"requestPayment", "torrent",
		"versioning", "versionId", "versions", "website",
		"uploads", "uploadId", "response-content-type",
		"response-content-language", "response-expires",
		"response-cache-control", "response-content-disposition",
		"response-content-encoding", "delete", "lifecycle",
		"tagging", "restore", "storageClass", "notification",
		"replication", "analytics", "metrics",
		"inventory", "select", "select-type",
	}
	sort.Strings(interestingResourcesContainer)
	// check for duplicates in the array - if it happens it is a programmer error that will happen only when that
	// query parameter is used - may be very hard to find.
	tempMap := map[string]bool{}
	var sortedArray []string
	for _, word := range interestingResourcesContainer {
		if _, ok := tempMap[word]; ok {
			logging.Default().
				WithField("word", word).
				Warn("appears twice in sig\v2.go array interestingResourcesContainer. a programmer error")
		} else {
			tempMap[word] = true
		}
	}
	for key := range tempMap {
		sortedArray = append(sortedArray, key)
	}
	sort.Strings(sortedArray)
	interestingResources = sortedArray
}

type v2Context struct {
	accessKeyID string
	signature   []byte
}

func (a v2Context) GetAccessKeyID() string {
	return a.accessKeyID
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
			logging.Default().WithField("header", headerValue).Error("log header does not match v2 structure")
			return ctx, ErrHeaderMalformed
		}
		result := make(map[string]string)
		for i, name := range V2AuthHeaderRegexp.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
		ctx.accessKeyID = result["AccessKeyId"]
		// parse signature
		sig, err := base64.StdEncoding.DecodeString(result["Signature"])
		if err != nil {
			logging.Default().WithField("header", headerValue).Error("log header does not match v2 structure (isn't proper base64)")
			return ctx, ErrHeaderMalformed
		}
		ctx.signature = sig
	}
	a.ctx = ctx
	return ctx, nil
}

func headerValueToString(val []string) string {
	var returnStr string
	for i, item := range val {
		if i == 0 {
			returnStr = strings.TrimSpace(item)
		} else {
			returnStr += "," + strings.TrimSpace(item)
		}
	}
	return returnStr
}

func canonicalStandardHeaders(headers http.Header) string {
	var returnStr string
	for _, hoi := range interestingHeaders {
		foundHoi := false
		for key, val := range headers {
			if len(val) > 0 && strings.ToLower(key) == hoi {
				returnStr += headerValueToString(val) + "\n"
				foundHoi = true
				break
			}
		}
		if !foundHoi {
			returnStr += "\n"
		}
	}
	return returnStr
}

func canonicalCustomHeaders(headers http.Header) string {
	var returnStr string
	var foundKeys []string
	for key := range headers {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-") {
			foundKeys = append(foundKeys, key)
		}
	}
	if len(foundKeys) == 0 {
		return returnStr
	}
	sort.Strings(foundKeys)
	for _, key := range foundKeys {
		returnStr += fmt.Sprint(strings.ToLower(key), ":", headerValueToString(headers[key]), "\n")
	}
	return returnStr
}

func canonicalResources(query url.Values, authPath string) string {
	var foundResources []string
	var foundResourcesStr string
	lowercaseQuery := make(url.Values)
	if len(query) > 0 {
		for key, val := range query {
			lowercaseQuery[strings.ToLower(key)] = val
		}
		for _, r := range interestingResources { // the resulting array will be sorted by resource name, because interesting resources array is sorted
			val, ok := lowercaseQuery[r]
			if ok {
				newValue := r
				if len(strings.Join(val, "")) > 0 {
					newValue += "=" + strings.Join(val, ",")
				}
				foundResources = append(foundResources, newValue)
			}
		}
		if len(foundResources) > 0 {
			foundResourcesStr = "?" + strings.Join(foundResources, "&")
		}
	}
	return authPath + foundResourcesStr
}

func canonicalString(method string, query url.Values, path string, headers http.Header) string {
	cs := strings.ToUpper(method) + "\n"
	cs += canonicalStandardHeaders(headers)
	cs += canonicalCustomHeaders(headers)
	cs += canonicalResources(query, path)
	return cs
}

func signCanonicalString(msg string, signature []byte) (digest []byte) {
	h := hmac.New(sha1.New, signature)
	_, _ = h.Write([]byte(msg))
	digest = h.Sum(nil)
	return
}

func buildPath(host string, bareDomain string, path string) string {
	h := httputil.HostOnly(host)
	b := httputil.HostOnly(bareDomain)
	if h == b {
		return path
	}
	bareSuffix := "." + b
	if strings.HasSuffix(h, bareSuffix) {
		prePath := strings.TrimSuffix(h, bareSuffix)
		return "/" + prePath + path
	}
	// bareDomain is not suffix of the path probably a bug
	logging.Default().
		WithFields(logging.Fields{"request_host": host, "bare_domain": bareDomain}).
		Error("request host mismatch")
	return ""
}

func (a *V2SigAuthenticator) Verify(creds *model.Credential, bareDomain string) error {
	/*
		s3 sigV2 implementation:
		the s3 signature  is somewhat different than general aws signature implementation.
		in boto3 configuration their value is 's3' and 's3v4' respectively, while the general aws signatures are
		'v2' and 'v4'.
		in 2020, the GO aws sdk does not inplement 's3' signature, So i will "translate" it from boto3.
		source is class botocore.auth.HmacV1Auth
		steps in building the string to be signed:
		1. create initial string, with uppercase http method + '\n'
		2. collect all required headers(in order):
			- standard headers - 'content-md5', 'content-type', 'date' - if one of those does not appear, it is replaces with an
			empty line '\n'. sorted and stringified
			- custom headers - any header that starts with 'x-amz-'. if the header appears more than once - the values
			are joined with ',' separator. sorted and stringified.
			- path of the object
			- QSA(Query String Arguments) - query arguments are searched for "interesting Resources". */

	/*
		URI encoding requirements for aws signature are different from what GO does.
		This logic is taken from https://docs.aws.amazon.com/AWSECommerceService/latest/DG/Query_QueryAuth.html
		This replacements are necessary for Java. There is no description about GO, but I found the '=' needs treatment as well
	*/

	patchedPath := strings.ReplaceAll(a.r.URL.Path, "=", "%3D")
	patchedPath = strings.ReplaceAll(patchedPath, "+", "%20")
	patchedPath = strings.ReplaceAll(patchedPath, "*", "%2A")
	patchedPath = strings.ReplaceAll(patchedPath, "%7E", "~")
	path := buildPath(a.r.Host, bareDomain, patchedPath)
	stringToSigh := canonicalString(a.r.Method, a.r.URL.Query(), path, a.r.Header)
	digest := signCanonicalString(stringToSigh, []byte(creds.AccessSecretKey))
	if !Equal(digest, a.ctx.signature) {
		return errors.ErrSignatureDoesNotMatch
	}
	return nil
}

func (a *V2SigAuthenticator) String() string {
	return "sigv2"
}
