package apiutil

const BaseURL = "/api/v1"

const LakeFSMetadataPrefix = "::lakefs::"

// metadata keys
const (
	SymlinkMetadataKey     = LakeFSMetadataPrefix + "symlink-target"
	ClientMtimeMetadataKey = LakeFSMetadataPrefix + "client-mtime"
)

const (
	LakeFSHeaderInternalPrefix = "x-lakefs-internal-"
	LakeFSHeaderMetadataPrefix = "x-lakefs-meta-"
)
