package diagnostics

import (
	"archive/zip"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCollector_Collector(t *testing.T) {
	conn, _ := testutil.GetDB(t, databaseURI)

	collector := NewCollector(conn)

	var out bytes.Buffer
	err := collector.Collect(context.Background(), &out)
	if err != nil {
		t.Fatal("Failed to collect data from database", err)
	}

	reader, err := zip.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal("Reader for collected zip", err)
	}

	// match part of the collected data we expect to have
	var names []string
	for _, file := range reader.File {
		names = append(names, file.Name)
	}

	expectedFiles := []string{
		"schema_migrations.csv",
		"auth_installation_metadata.csv",
		"auth_users_count.csv",
		"table_sizes.csv",
	}
	for _, expectedFile := range expectedFiles {
		found := false
		for _, name := range names {
			if name == expectedFile {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Collected data file '%s' was expected in [%s]", expectedFile, strings.Join(names, ","))
		}
	}
}
