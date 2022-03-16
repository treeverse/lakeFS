package ddl

import "embed"

//go:embed *.sql
var Content embed.FS
