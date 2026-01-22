package webui

import (
	"embed"
	"io/fs"
)

// Content embeds the webui files: fallback.html and the dist directory.
// The dist directory may only contain .gitkeep if the UI hasn't been built.
// Note: dist/.gitkeep must exist for the embed to work; make clean should preserve it.
//
//go:embed fallback.html all:dist
var Content embed.FS

// UIBuilt is true if the full UI was built (dist/index.html exists)
var UIBuilt bool

func init() {
	// Check if dist/index.html exists (only present after npm run build)
	_, err := fs.Stat(Content, "dist/index.html")
	UIBuilt = err == nil
}
