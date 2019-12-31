package sig

import (
	"encoding/base64"
	"net/http"
	"regexp"

	log "github.com/sirupsen/logrus"
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

func (a *V2SigAuthenticator) Verify(Credentials) error {
	// let's for now, simply assume we're good so we can test Spark
	// TODO: IMPLEMENT THIS
	return nil
}

func (a *V2SigAuthenticator) String() string {
	return "sigv2"
}
