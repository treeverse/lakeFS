package osstats

import (
	"fmt"
	"strings"
	"time"
)

const (
	retryWaitTime    = 500 * time.Millisecond
	brokenPipeOutput = "broken pipe"
)

type OSStats struct {
	Platform string
	OS       string
	Version  string
}

func (os *OSStats) String() string {
	return fmt.Sprintf("Platform: %s, OS: %s, Version: %s", os.Platform, os.OS, os.Version)
}

func replaceLineTerminations(s string) string {
	s = strings.ReplaceAll(s, "\n", "")
	return strings.ReplaceAll(s, "\r\n", "")
}
