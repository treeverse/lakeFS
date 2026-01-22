package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

//go:embed fallback.html
var fallbackHTML []byte

// Content provides the dist filesystem (may only contain .gitkeep if UI not built)
var Content fs.FS

// UIBuilt is true if the full UI was built (dist/index.html exists)
var UIBuilt bool

// FallbackHTML returns HTML to serve when UI is not built
func FallbackHTML() []byte {
	return fallbackHTML
}

func init() {
	// Get the dist subdirectory
	Content, _ = fs.Sub(distFS, "dist")

	// Check if index.html exists (only present after npm run build)
	_, err := fs.Stat(Content, "index.html")
	UIBuilt = err == nil
}
