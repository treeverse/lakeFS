package actions

import "context"

type FileRef struct {
	Path    string
	Address string
}

type Source interface {
	List(ctx context.Context) ([]FileRef, error)
	Load(ctx context.Context, name FileRef) ([]byte, error)
}
