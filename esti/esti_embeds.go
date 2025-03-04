package esti

import "embed"

// A place for embedding files for use in ESTI tests

//go:embed action_files/*.yaml
var ActionsPath embed.FS

//go:embed all:export_hooks_files
var ExportHooksFiles embed.FS
