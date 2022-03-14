package ddl

import "embed"

// Content embeds database migration sql files
//go:embed *.sql
var Content embed.FS
