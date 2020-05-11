package local

import (
	"encoding/hex"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
)

const PartsNo = 30

func TestEtag(t *testing.T) {
	var base [16]byte
	b := base[:]
	parts := make([]*s3.CompletedPart, PartsNo)
	for i := 0; i < PartsNo; i++ {

		for j := 0; j < len(b); j++ {
			b[j] = byte(32 + i + j)
		}
		s := hex.EncodeToString(b)
		p := new(s3.CompletedPart)
		p.ETag = &s
		parts[i] = p
	}
	ETag := computeETag(parts)
	if ETag != "9cae1a3b7e97542c261cf2e1b50ba482" {
		t.Fatal("ETag value " + ETag + " not as expected")
	}
}
