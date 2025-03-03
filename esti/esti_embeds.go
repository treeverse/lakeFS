package esti

import "embed"

//go:embed action_files/*.yaml
var ActionsPath embed.FS
