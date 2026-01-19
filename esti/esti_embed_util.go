package esti

import "embed"

// A place for embedding files for use in ESTI tests

//go:embed action_files/*.yaml action_files/scripts/*.lua
var ActionsPath embed.FS

//go:embed all:export_hooks_files
var ExportHooksFiles embed.FS

//go:embed files/*
var TestFiles embed.FS
