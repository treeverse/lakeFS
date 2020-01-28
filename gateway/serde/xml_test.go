package serde_test

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/gateway/serde"
)

func TestTimestamp(t *testing.T) {
	var ts int64 = 1000197960
	expected := "2001-09-11T08:46:00.000Z"
	got := serde.Timestamp(ts)
	if !strings.EqualFold(got, expected) {
		t.Fatalf("expected %s, got %s for ts = %d", expected, got, ts)
	}
}

func TestMarshal(t *testing.T) {
	response := serde.ListAllMyBucketsResult{
		Buckets: serde.Buckets{
			Bucket: []serde.Bucket{
				serde.Bucket{
					CreationDate: "2001-09-11T08:46:00.000Z",
					Name:         "bucket 1",
				}, serde.Bucket{
					CreationDate: "2001-09-11T08:46:00.000Z",
					Name:         "bucket 1",
				},
			},
		},
		Owner: serde.Owner{
			DisplayName: "Oz Katz",
			ID:          "abcdefg",
		},
	}
	data, err := xml.MarshalIndent(response, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "<Buckets>") {
		t.Fatalf("expected a buckets array")
	}
}
