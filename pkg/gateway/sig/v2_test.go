package sig

import (
	"testing"
)

func Test_buildPath(t *testing.T) {
	type args struct {
		host       string
		bareDomain string
		path       string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "exact", args: args{host: "s3.host.com", bareDomain: "s3.host.com", path: "/path/1"}, want: "/path/1"},
		{name: "sub1", args: args{host: "sub1.s3.host.com", bareDomain: "s3.host.com", path: "/path/1"}, want: "/sub1/path/1"},
		{name: "sub.domain", args: args{host: "sub.domain.s3.host.com", bareDomain: "s3.host.com", path: "/path/1"}, want: "/sub.domain/path/1"},
		{name: "no match", args: args{host: "s3.host.com", bareDomain: "s3.host.io", path: "/path/1"}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildPath(tt.args.host, tt.args.bareDomain, tt.args.path); got != tt.want {
				t.Errorf("buildPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
