package metastore

import "testing"

func TestReplaceBranchName(t *testing.T) {
	type args struct {
		location string
		branch   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "table",
			args: args{
				location: "s3a://repo/master/path/to/table",
				branch:   "br1",
			},
			want: "s3a://repo/br1/path/to/table",
		},
		{
			name: "partition",
			args: args{
				location: "s3a://repo/master/path/to/table/partition=value",
				branch:   "br1",
			},
			want: "s3a://repo/br1/path/to/table/partition=value",
		},
		{
			name: "error returns location input",
			args: args{
				location: "~s3:/s:@12/%?",
				branch:   "br1",
			},
			want: "~s3:/s:@12/%?",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReplaceBranchName(tt.args.location, tt.args.branch); got != tt.want {
				t.Errorf("ReplaceBranchName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSymlinkLocation(t *testing.T) {
	type args struct {
		location       string
		locationPrefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "table location - repo is bucket",
			args: args{
				location:       "s3a://repo/master/tableName",
				locationPrefix: "s3://bucket/lakeFS",
			},
			want: "s3://bucket/lakeFS/repo/master/tableName",
		},
		{
			name: "partition location - repo is bucket",
			args: args{
				location:       "s3a://repo/master/tableName/partition=value",
				locationPrefix: "s3://bucket/lakeFS",
			},
			want: "s3://bucket/lakeFS/repo/master/tableName/partition=value",
		},
		{
			name: "partition location - repo is path",
			args: args{
				location:       "s3a://repo/master/tableName/partition=value",
				locationPrefix: "s3://bucket/some/path/lakeFS",
			},
			want: "s3://bucket/some/path/lakeFS/repo/master/tableName/partition=value",
		},
		{
			name: "error returns location input",
			args: args{
				location:       "~s3:/s:@12/%?",
				locationPrefix: "s3://bucket/some/path/lakeFS",
			},
			want: "~s3:/s:@12/%?",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSymlinkLocation(tt.args.location, tt.args.locationPrefix); got != tt.want {
				t.Errorf("GetSymlinkLocation() = %v, want %v", got, tt.want)
			}
		})
	}
}
