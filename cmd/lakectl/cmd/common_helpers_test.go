package cmd

import "testing"

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
		{name: "access key id with wrong suffiix", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZA"}, want: false},
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

func TestIsBase64(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid base64", args: args{s: "TQG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: true},
		{name: "invalid base64", args: args{s: "!QG5JcovOozCGJnIRmIKH7Flq1tLxrUbyi9/WmJy"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBase64(tt.args.s); got != tt.want {
				t.Errorf("IsBase64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsValidURI(t *testing.T) {
	type args struct {
		uri string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid uri", args: args{uri: "lakefs://main.com/side"}, want: true},
		{name: "invalid uri", args: args{uri: "not_uri"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidURI(tt.args.uri); got != tt.want {
				t.Errorf("IsValidURI() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsValidEndpointURI(t *testing.T) {
	type args struct {
		uri string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{name: "valid api uri", args: args{uri: "lakefs://main.com/side/api/v1"}, want: true},
		{name: "invalid api uri", args: args{uri: "lakefs://main.com/side/api/v11"}, want: false},
		{name: "invalid uri", args: args{uri: "not_uri"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidEndpointURI(tt.args.uri); got != tt.want {
				t.Errorf("IsValidEndpointURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
