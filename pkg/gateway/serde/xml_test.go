package serde_test

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/gateway/serde"
)

func TestTimestamp(t *testing.T) {
	const (
		ts       = 1000197960
		expected = "2001-09-11T08:46:00Z"
	)
	got := serde.Timestamp(time.Unix(ts, 0))
	if got != expected {
		t.Fatalf("expected %s, got %s for ts = %d", expected, got, ts)
	}
}

func TestMarshal(t *testing.T) {
	response := serde.ListAllMyBucketsResult{
		Buckets: serde.Buckets{
			Bucket: []serde.Bucket{
				{
					CreationDate: "2001-09-11T08:46:00.000Z",
					Name:         "bucket 1",
				},
				{
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
