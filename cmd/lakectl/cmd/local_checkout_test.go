package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckoutSwitchesRef(t *testing.T) {
	tests := []struct {
		name         string
		currentRef   string
		currentHead  string
		requestedRef string
		newHead      string
		want         bool
	}{
		{
			name:         "same ref same commit",
			currentRef:   "main",
			currentHead:  "c1",
			requestedRef: "main",
			newHead:      "c1",
			want:         false,
		},
		{
			name:         "different ref same commit",
			currentRef:   "branch-a",
			currentHead:  "c1",
			requestedRef: "branch-b",
			newHead:      "c1",
			want:         true,
		},
		{
			name:         "same ref new commit",
			currentRef:   "main",
			currentHead:  "c1",
			requestedRef: "main",
			newHead:      "c2",
			want:         true,
		},
		{
			name:         "different ref different commit",
			currentRef:   "branch-a",
			currentHead:  "c1",
			requestedRef: "branch-b",
			newHead:      "c2",
			want:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkoutSwitchesRef(tt.currentRef, tt.currentHead, tt.requestedRef, tt.newHead)
			require.Equal(t, tt.want, got)
		})
	}
}
