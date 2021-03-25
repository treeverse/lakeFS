package metastore

import "testing"

func TestReplaceBranchName(t *testing.T) {
	type args struct {
		location string
		branch   string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
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
			name: "no schema",
			args: args{
				location: "noschema/repo",
				branch:   "br1",
			},
			wantErr: true,
		},
		{
			name: "only schema",
			args: args{
				location: "s3://",
				branch:   "br1",
			},
			wantErr: true,
		},
		{
			name: "invalud url",
			args: args{
				location: "~s3:/s:@12/%?",
				branch:   "br1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReplaceBranchName(tt.args.location, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReplaceBranchName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReplaceBranchName() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSymlinkLocation1(t *testing.T) {
	type args struct {
		location       string
		locationPrefix string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
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
			name: "no schema",
			args: args{
				location:       "noschema/repo",
				locationPrefix: "s3://bucket/some/path/lakeFS",
			},
			wantErr: true,
		},
		{
			name: "only schema",
			args: args{
				location:       "s3://",
				locationPrefix: "s3://bucket/some/path/lakeFS",
			},
			wantErr: true,
		},
		{
			name: "invalid url",
			args: args{
				location:       "~s3:/s:@12/%?",
				locationPrefix: "s3://bucket/some/path/lakeFS",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetSymlinkLocation(tt.args.location, tt.args.locationPrefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSymlinkLocation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetSymlinkLocation() got = %v, want %v", got, tt.want)
			}
		})
	}
}
