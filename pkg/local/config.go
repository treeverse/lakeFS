package local

import "github.com/treeverse/lakefs/pkg/api/apiutil"

const (
	// DefaultDirectoryPermissions Octal representation of default folder permissions
	DefaultDirectoryPermissions = 0o040777
	ClientMtimeMetadataKey      = apiutil.LakeFSMetadataPrefix + "client-mtime"
)

type SyncFlags struct {
	Parallelism      int
	Presign          bool
	PresignMultipart bool
}

type Config struct {
	SyncFlags
	// IgnoreSymLinks - By default lakectl local fails if local directory contains a symbolic link. When set, lakectl will ignore the symbolic links instead.
	IgnoreSymLinks bool `mapstructure:"ignore_symlinks"`
	// IncludePerm - Experimental: preserve Unix file permissions
	IncludePerm bool
}
