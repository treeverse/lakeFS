package webui

import "embed"

// Content embeds web ui dist folder
//
//go:embed dist
var Content embed.FS
