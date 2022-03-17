package local

import (
	"encoding/hex"
	"testing"

	"github.com/treeverse/lakefs/pkg/block"
)

const PartsNo = 30

func TestEtag(t *testing.T) {
	var base [16]byte
	b := base[:]
	parts := make([]block.MultipartPart, PartsNo)
	for i := 0; i < PartsNo; i++ {
		for j := 0; j < len(b); j++ {
			b[j] = byte(32 + i + j)
		}
		parts[i].PartNumber = i + 1
		parts[i].ETag = hex.EncodeToString(b)
	}
	etag := computeETag(parts)
	if etag != "9cae1a3b7e97542c261cf2e1b50ba482" {
		t.Fatalf("ETag value '%s' not as expected", etag)
	}
}
