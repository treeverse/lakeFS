package cmd

import (
	"strings"
	"testing"

	"github.com/jedib0t/go-pretty/v6/text"
)

func TestIsValidAccessKeyID(t *testing.T) {
	type args struct {
		accessKeyID string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid access key id", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZQ"}, want: true},
		{name: "access key id with lower case char", args: args{accessKeyID: "AKIAJ12zZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with invalid char", args: args{accessKeyID: "AKIAJ12!ZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with extra char", args: args{accessKeyID: "AKIAJ123ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with missing char", args: args{accessKeyID: "AKIAJ1ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong prefix", args: args{accessKeyID: "AKIAM12ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong suffix", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZA"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidAccessKeyID(tt.args.accessKeyID); got != tt.want {
				t.Errorf("IsValidAccessKeyID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsValidSecretAccessKey(t *testing.T) {
	type args struct {
		secretAccessKey string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid secret access key", args: args{secretAccessKey: "TQG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: true},
		{name: "secret access key id with invalid char", args: args{secretAccessKey: "!QG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
		{name: "secret access key id with extra char", args: args{secretAccessKey: "aTQG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
		{name: "secret access key id with missing char", args: args{secretAccessKey: "QG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidSecretAccessKey(tt.args.secretAccessKey); got != tt.want {
				t.Errorf("IsValidSecretAccessKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestColors(t *testing.T) {
	text.EnableColors()
	defer text.DisableColors()
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{name: "plain", template: `abc`, want: "abc"},
		{name: "red", template: `{{"abc" | red}}def`, want: "\x1b[91mabc\x1b[0mdef"},
		{name: "yellow", template: `{{"abc" | yellow}}def`, want: "\x1b[93mabc\x1b[0mdef"},
		{name: "green", template: `{{"abc" | green}}def`, want: "\x1b[92mabc\x1b[0mdef"},
		{name: "blue", template: `{{"abc" | blue}}def`, want: "\x1b[94mabc\x1b[0mdef"},
		{name: "bold", template: `{{"abc" | bold}}def`, want: "\x1b[1mabc\x1b[0mdef"},
		{name: "underline", template: `{{"abc" | underline}}def`, want: "\x1b[4mabc\x1b[0mdef"},
		{name: "boldgreen", template: `{{"abc" | bold | green}}def`, want: "\x1b[1;92mabc\x1b[0mdef"},
		{name: "greenbold", template: `{{"abc" | green | bold}}def`, want: "\x1b[92;1mabc\x1b[0mdef"},
		{name: "redunderline", template: `{{"abc" | red | underline}}def`, want: "\x1b[91;4mabc\x1b[0mdef"},
		{name: "red-number", template: `{{2 | red}}`, want: "\x1b[91m2\x1b[0m"},
		{name: "red-pointer", template: `{{. | red}}`, want: "\x1b[91mxyzzy\x1b[0m"},
	}

	text := "xyzzy"
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := strings.Builder{}
			WriteTo(tc.template, &text, &w)
			got := w.String()
			if got != tc.want {
				t.Errorf("%s got %q want %q", tc.template, got, tc.want)
			}
			// Show off the nice colors!
			t.Log(got)
		})
	}
}
