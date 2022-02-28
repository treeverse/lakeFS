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
		{name: "valid access key id", args: args{accessKeyID: "AKIAJ22ZZZZZZZZZZZZQ"}, want: true},
		{name: "access key id with lower case char", args: args{accessKeyID: "AKIAJ22zZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with invalid char", args: args{accessKeyID: "AKIAJ22!ZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with extra char", args: args{accessKeyID: "AKIAJ223ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with missing char", args: args{accessKeyID: "AKIAJ2ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong prefix", args: args{accessKeyID: "AKIAM22ZZZZZZZZZZZZQ"}, want: false},
		{name: "access key id with wrong suffiix", args: args{accessKeyID: "AKIAJ22ZZZZZZZZZZZZA"}, want: false},
		{name: "access key id with invalid number", args: args{accessKeyID: "AKIAJ12ZZZZZZZZZZZZQ"}, want: false},
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
