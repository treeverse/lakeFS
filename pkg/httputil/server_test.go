package httputil

import (
	"testing"
)

func TestSubdomainsOf(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name string
		args args
		host string
		want bool
	}{
		{name: "small", args: args{v: "s3.local.io"}, host: "s3", want: false},
		{name: "extract", args: args{v: "s3.local.io"}, host: "s3.local.io", want: false},
		{name: "no sub", args: args{v: "s3.local.io"}, host: ".s3.local.io", want: false},
		{name: "dot sub", args: args{v: "s3.local.io"}, host: "..s3.local.io", want: false},
		{name: "invalid1", args: args{v: "s3.local.io"}, host: "1.asdfsa.s3.local.io", want: false},
		{name: "invalid2", args: args{v: "s3.local.io"}, host: ".asdfsa.s3.local.io", want: false},
		{name: "subdomain", args: args{v: "s3.local.io"}, host: "asdfsa.s3.local.io", want: true},
		{name: "subdomain port", args: args{v: "s3.local.io:8000"}, host: "sub.s3.local.io", want: true},
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
		v string
	}
	tests := []struct {
		name string
		args args
		host string
		want bool
	}{
		{name: "small", args: args{v: "s3.local.io"}, host: "s3", want: false},
		{name: "extract", args: args{v: "s3.local.io"}, host: "s3.local.io", want: true},
		{name: "subdomain", args: args{v: "s3.local.io"}, host: "sub.s3.local.io", want: false},
		{name: "empty", args: args{v: "s3.local.io"}, host: "", want: false},
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
