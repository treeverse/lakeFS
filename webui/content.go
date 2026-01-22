package webui

import (
	"embed"
	"io/fs"
)

// content embeds the dist directory for serving the web UI.
// The dist directory may only contain .gitkeep if the UI hasn't been built.
// Note: dist/.gitkeep must exist for the embed to work; make clean preserves it.
//
//go:embed all:dist
var content embed.FS

// Content provides the dist filesystem for serving the web UI.
var Content fs.FS

// FallbackHTML is served when the UI has not been built.
//
//go:embed fallback.html
var FallbackHTML []byte

// UIBuilt is true if the full UI was built (dist/index.html exists)
var UIBuilt bool

func init() {
	// Get the dist subdirectory
	Content, _ = fs.Sub(content, "dist")

	// Check if index.html exists (only present after npm run build)
	_, err := fs.Stat(Content, "index.html")
	UIBuilt = err == nil
}
