package apiutil

const BaseURL = "/api/v1"

const LakeFSMetadataPrefix = "::lakefs::"

// metadata keys
const (
	ClientMtimeMetadataKey      = LakeFSMetadataPrefix + "client-mtime"
	SymlinkMetadataKey          = LakeFSMetadataPrefix + "symlink-target"
	POSIXPermissionsMetadataKey = LakeFSMetadataPrefix + "posix-permissions"
)

const (
	LakeFSHeaderInternalPrefix = "x-lakefs-internal-"
	LakeFSHeaderMetadataPrefix = "x-lakefs-meta-"
)
