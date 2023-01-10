package upload

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/xid"
)

func TestPathPartitionProvider(t *testing.T) {
	t.Run("by_time", func(t *testing.T) {
		const (
			times    = 3
			size     = 3000
			interval = time.Second
			prefix   = "by_time"
		)
		tm := time.Now()
		tellTime := func() time.Time { return tm }
		provider := NewPathPartitionProvider(
			WithPathProviderPrefix(prefix),
			WithPathProviderSize(size),
			WithPathProviderInterval(interval),
			WithPathProviderTellTime(tellTime),
		)

		for i := 0; i < times; i++ {
			path1 := provider.NewPath()
			tm = tm.Add(2 * interval)
			path2 := provider.NewPath()

			testSmallerUploadPath(t, prefix, path1, path2, true)
		}
	})

	t.Run("by_size", func(t *testing.T) {
		const (
			times    = 3
			size     = 5
			interval = 24 * time.Hour
			prefix   = "by_size"
		)
		tm := time.Now()
		tellTime := func() time.Time { return tm }
		provider := NewPathPartitionProvider(
			WithPathProviderPrefix(prefix),
			WithPathProviderSize(size),
			WithPathProviderInterval(interval),
			WithPathProviderTellTime(tellTime),
		)

		for i := 0; i < times; i++ {
			// first series will have the same partition
			first := make([]string, size)
			for n := 0; n < size; n++ {
				first[n] = provider.NewPath()
			}
			for n := 1; n < size; n++ {
				testSmallerUploadPath(t, prefix, first[n-1], first[n], false)
			}

			// we need to assume that at lease 1 second passed.
			// in real like the folder from the last X minutes will not be processed
			tm = tm.Add(time.Second)

			// second series will have the same partition
			second := make([]string, size)
			for n := 0; n < size; n++ {
				second[n] = provider.NewPath()
			}
			for n := 1; n < size; n++ {
				testSmallerUploadPath(t, prefix, second[n-1], second[n], false)
			}

			// verify partition switch between series
			testSmallerUploadPath(t, prefix, first[len(first)-1], second[0], true)
		}
	})
}

func testSmallerUploadPath(t *testing.T, prefix, path1, path2 string, includePartition bool) {
	t.Helper()
	p1 := testValidUploadPath(t, prefix, path1)
	p2 := testValidUploadPath(t, prefix, path2)
	if p1[0] != p2[0] {
		t.Errorf("Prefix doesn't match: '%s' '%s'", path1, path2)
	}
	if includePartition && p1[1] < p2[1] {
		t.Errorf("Partition should be smaller: '%s' '%s'", path1, path2)
	}
	if p1[2] >= p2[2] {
		t.Errorf("ID should be smaller: '%s' '%s'", path1, path2)
	}
}

func testValidUploadPath(t *testing.T, prefix, p string) []string {
	t.Helper()
	if !strings.HasPrefix(p, prefix+"/") {
		t.Fatalf("Expected prefix '%s': %s", prefix, p)
	}
	const expectedParts = 3
	parts := strings.Split(p, "/")
	if len(parts) != expectedParts {
		t.Fatalf("Expected %d separators: %s", expectedParts, p)
	}
	return parts
}

func TestResolvePathTime(t *testing.T) {
	const (
		size     = 5
		interval = 24 * time.Hour
		prefix   = "by_size"
	)
	tm := time.Now()
	tellTime := func() time.Time { return tm }
	provider := NewPathPartitionProvider(
		WithPathProviderPrefix(prefix),
		WithPathProviderSize(size),
		WithPathProviderInterval(interval),
		WithPathProviderTellTime(tellTime),
	)

	t.Run("sanity", func(t *testing.T) {
		time := time.Now()
		id := xid.NewWithTime(time).String()

		idTime, err := provider.ResolvePathTime(id)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if time.Equal(idTime) {
			t.Errorf("time doesn't match: '%s' '%s'", idTime, time)
		}
	})

	t.Run("string non xid", func(t *testing.T) {
		id := "test"

		_, err := provider.ResolvePathTime(id)
		if !errors.Is(err, xid.ErrInvalidID) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
