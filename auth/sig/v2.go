package sig

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	str "strings"
)

const (
	v2authHeaderName = "Authorization"
)

var (
	V2AuthHeaderRegexp = regexp.MustCompile(`AWS (?P<AccessKeyId>[A-Z0-9]{20}):(?P<Signature>[A-Za-z0-9+/=]+)`)
	// Both "interesting" arrays are sorted. so once we extract relevant items by looping on them = the result is sorted
	interestingHeaders = [...]string{"content-md5", "content-type", "date"}
	//sorted by an init function
	interestingResources = []string{"accelerate", "acl", "cors", "defaultObjectAcl",
		"location", "logging", "partNumber", "policy",
		"requestPayment", "torrent",
		"versioning", "versionId", "versions", "website",
		"uploads", "uploadId", "response-content-type",
		"response-content-language", "response-expires",
		"response-cache-control", "response-content-disposition",
		"response-content-encoding", "delete", "lifecycle",
		"tagging", "restore", "storageClass", "notification",
		"replication", "analytics", "metrics",
		"inventory", "select", "select-type"}
)

func init() {
	sort.Strings(interestingResources)
}

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

func headerValueToString(val []string) string {
	var returnStr string
	for i, item := range val {
		if i == 0 {
			returnStr = str.TrimSpace(item)
		} else {
			returnStr += "," + str.TrimSpace(item)
		}
	}
	return returnStr
}

func canonicalStandardHeaders(headers http.Header) string {
	var returnStr string
	for _, hoi := range interestingHeaders {
		foundHoi := false
		for key, val := range headers {
			if len(val) > 0 && str.ToLower(key) == hoi {
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
	for key, _ := range headers {
		if str.HasPrefix(str.ToLower(key), "x-amz-") {
			foundKeys = append(foundKeys, key)
		}
	}
	if len(foundKeys) == 0 {
		return returnStr
	}
	sort.Strings(foundKeys)
	for _, key := range foundKeys {
		returnStr += fmt.Sprint(str.ToLower(key), ":", headerValueToString(headers[key]), "\n")
	}
	return returnStr
}

func canonicalResources(query url.Values, authPath string) string {
	var foundResources []string
	var foundResourcesStr string
	lowercaseQuery := make(url.Values)
	if len(query) > 0 {
		for key, val := range query {
			lowercaseQuery[str.ToLower(key)] = val
		}
		for _, r := range interestingResources { // the resulting array will be sorted by resource name, because interesting resources array is sorted
			val, ok := lowercaseQuery[r]
			if ok {
				newValue := r + "=" + str.Join(val, ",")
				foundResources = append(foundResources, newValue)
			}
		}
		if len(foundResources) > 0 {
			foundResourcesStr = "?" + str.Join(foundResources, "&")
		}
	}
	return authPath + foundResourcesStr
}

func canonicalString(method string, query url.Values, path string, headers http.Header) string {
	cs := str.ToUpper(method) + "\n"
	cs += canonicalStandardHeaders(headers)
	cs += canonicalCustomHeaders(headers)
	cs += canonicalResources(query, path)
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
		2. collect all required headers(in order):
			- standard headers - 'content-md5', 'content-type', 'date' - if one of those does not appear, it is replaces with an
			empty line '\n'. sorted and stringified
			- custom headers - any header that starts with 'x-amz-'. if the header appears more than once - the values
			are joined with ',' seperator. sorted and stringified.
			- path of the object
			- QSA(Query String Arguments) - query arguments are searched for "interestin Resources".
	*/
	var path string
	hostParts := str.Split(a.r.Host, ".")
	if hostParts[1] == "s3" {
		path = hostParts[1] + "/" + a.r.URL.Path
	} else {
		path = a.r.URL.Path
	}
	stringToSigh := canonicalString(a.r.Method, a.r.URL.Query(), path, a.r.Header)
	fmt.Print(stringToSigh, "\n")
	digest := signCanonicalString(stringToSigh, []byte(creds.GetAccessSecretKey()))
	if !hmac.Equal(digest, a.ctx.signature) {
		return ErrBadSignature
	}
	return nil
}

func (a *V2SigAuthenticator) String() string {
	return "sigv2"
}
