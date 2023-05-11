package assets

import "embed"

var (
	//go:embed all:sample
	SampleData embed.FS
)
