package ident_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/ident"
)

type IdentifiableString string

func (i IdentifiableString) Identity() []byte {
	return []byte(i)
}

func TestHash(t *testing.T) {
	data := []struct {
		Input    string
		Expected string
	}{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"hello world", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"},
		{"something completely different", "0bcd9fcb4f42a66858a11d8bccde9781b3e5368e9cb36120dbabf32b1159fafb"},
		{"shouldn't this be a constant size for the hash? how is it not?", "1b4bc8d9d701eebe4dc0256e7334605ef0cb142336159f49bf31fdc0bdda0819"},
	}

	for _, tc := range data {
		got := ident.Hash(IdentifiableString(tc.Input))
		if !strings.EqualFold(got, tc.Expected) {
			t.Fatalf("for input: \"%s\", expected \"%s\", got \"%s\"", tc.Input, tc.Expected, got)
		}
	}
}

func printStrings(strs []string) string {
	quoted := make([]string, len(strs))
	for i, str := range strs {
		quoted[i] = fmt.Sprintf("\"%s\"", str)
	}
	joined := strings.Join(quoted, ", ")
	return fmt.Sprintf("[%s]", joined)
}

func TestMultiHash(t *testing.T) {
	data := []struct {
		Input    []string
		Expected string
	}{
		{[]string{"", ""}, "3b7546ed79e3e5a7907381b093c5a182cbf364c5dd0443dfa956c8cca271cc33"},
		{[]string{"foo", "bar"}, "ec321de56af3b66fb49e89cfe346562388af387db689165d6f662a3950286a57"},
		{[]string{"foo", "bar"}, "ec321de56af3b66fb49e89cfe346562388af387db689165d6f662a3950286a57"},
		{[]string{""}, "cd372fb85148700fa88095e3492d3f9f5beb43e555e5ff26d95f5a6adc36f8e6"},
		{[]string{}, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{[]string{}, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	}

	for _, tc := range data {
		hashes := make([]string, len(tc.Input))
		for i, inp := range tc.Input {
			hashes[i] = ident.Hash(IdentifiableString(inp))
		}
		got := ident.MultiHash(hashes...)
		if !strings.EqualFold(got, tc.Expected) {
			t.Fatalf("for input: %s, expected \"%s\", got \"%s\"", printStrings(tc.Input), tc.Expected, got)
		}
	}
}
