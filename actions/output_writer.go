package actions

import (
	"context"
	"io"
)

type OutputWriter interface {
	OutputWrite(ctx context.Context, name string, reader io.Reader) error
}
