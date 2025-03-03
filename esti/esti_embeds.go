package esti

import "embed"

//go:embed action_files/*.yaml
var ActionsPath embed.FS

//go:embed all:export_hooks_files
var ExportHooksFiles embed.FS
