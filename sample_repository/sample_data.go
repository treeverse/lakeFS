package sample

import "embed"

var (
	//go:embed sample all:*
	SampleData embed.FS
)
