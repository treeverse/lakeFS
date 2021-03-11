package pyramid

import "errors"

var (
	errPathInWorkspace = errors.New("file cannot be located in the workspace")
	errEmptyDirInPath  = errors.New("file path cannot contain an empty directory")
	errFilePersisted   = errors.New("file is persisted")
	errFileAborted     = errors.New("file is aborted")
)
