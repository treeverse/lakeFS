package httputil

import (
	"testing"
)

func TestSubdomainsOf(t *testing.T) {
	type args struct {
		v []string
	}
	tests := []struct {
		name string
		args args
		host string
		want bool
	}{
		{name: "short", args: args{v: []string{"s3.local.io"}}, host: "s3", want: false},
		{name: "short many", args: args{v: []string{"s3.local.io", "s3.dev.invalid"}}, host: "s3", want: false},
		{name: "extract", args: args{v: []string{"s3.local.io"}}, host: "s3.local.io", want: false},
		{name: "extract many", args: args{v: []string{"s3.dev.invalid", "s3.local.io"}}, host: "s3.local.io", want: false},
		{name: "no sub", args: args{v: []string{"s3.local.io"}}, host: ".s3.local.io", want: false},
		{name: "no sub many", args: args{v: []string{"s3.dev.invalid", "s3.local.io"}}, host: ".s3.local.io", want: false},
		{name: "dot sub", args: args{v: []string{"s3.local.io"}}, host: "..s3.local.io", want: false},
		{name: "dot sub many", args: args{v: []string{"s3.local.io", "s3.dev.invalid"}}, host: "..s3.local.io", want: false},
		{name: "invalid1", args: args{v: []string{"s3.local.io"}}, host: "1.asdfsa.s3.local.io", want: false},
		{name: "invalid1 many", args: args{v: []string{"s3.local.io", "s3.dev.invalid"}}, host: "1.asdfsa.s3.local.io", want: false},
		{name: "invalid2", args: args{v: []string{"s3.local.io"}}, host: ".asdfsa.s3.local.io", want: false},
		{name: "invalid2 many", args: args{v: []string{"s3.local.io", "s3.dev.invalid"}}, host: ".asdfsa.s3.local.io", want: false},
		{name: "subdomain", args: args{v: []string{"s3.local.io"}}, host: "asdfsa.s3.local.io", want: true},
		{name: "subdomain many", args: args{v: []string{"s3.dev.invalid", "s3.local.io", "s3.example.net"}}, host: "asdfsa.s3.local.io", want: true},
		{name: "subdomain port", args: args{v: []string{"s3.local.io:8000"}}, host: "sub.s3.local.io", want: true},
		{name: "subdomain port many", args: args{v: []string{"s3.dev.invalid:2000", "s3.local.io:8000", "s3.example.net:9000"}}, host: "sub.s3.local.io", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := SubdomainsOf(tt.args.v)
			if got := m(tt.host); got != tt.want {
				t.Errorf("SubdomainsOf() '%s' test with '%s' got = %t, want = %t", tt.args.v, tt.host, got, tt.want)
			}
		})
	}
}

func TestExact(t *testing.T) {
	type args struct {
		v []string
	}
	tests := []struct {
		name string
		args args
		host string
		want bool
	}{
		{name: "short", args: args{v: []string{"s3.local.io"}}, host: "s3", want: false},
		{name: "short many", args: args{v: []string{"s3.local.io", "s3.dev.invalid"}}, host: "s3", want: false},
		{name: "extract", args: args{v: []string{"s3.local.io"}}, host: "s3.local.io", want: true},
		{name: "extract many", args: args{v: []string{"s3.dev.invalid", "s3.local.io", "s3.example.net"}}, host: "s3.local.io", want: true},
		{name: "subdomain", args: args{v: []string{"s3.local.io"}}, host: "sub.s3.local.io", want: false},
		{name: "subdomain many", args: args{v: []string{"s3.dev.invalid", "s3.local.io"}}, host: "sub.s3.local.io", want: false},
		{name: "empty", args: args{v: []string{"s3.local.io"}}, host: "", want: false},
		{name: "empty many", args: args{v: []string{"s3.dev.invalid", "s3.local.io"}}, host: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Exact(tt.args.v)
			if got := m(tt.host); got != tt.want {
				t.Errorf("Exact() '%s' test with '%s' got = %t, want = %t", tt.args.v, tt.host, got, tt.want)
			}
		})
	}
}
