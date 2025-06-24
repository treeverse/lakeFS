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
	NoProgress       bool
}

type Config struct {
	SyncFlags
	// MaxDownloadRetries - Maximum number of retries for download operations
	MaxDownloadRetries uint64
	// SkipNonRegularFiles - By default lakectl local fails if local directory contains irregular files. When set, lakectl will skip these files instead.
	SkipNonRegularFiles bool
	// IncludePerm - Experimental: preserve Unix file permissions
	IncludePerm bool
	IncludeUID  bool
	IncludeGID  bool
}
