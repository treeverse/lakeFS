package internal

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/git/errors"
)

func HandleOutput(out string, err error) (string, error) {
	lowerOut := strings.ToLower(out)
	switch {
	case err == nil:
		return strings.TrimSpace(out), nil
	case strings.Contains(lowerOut, "not a git repository"):
		return "", errors.ErrNotARepository
	case strings.Contains(lowerOut, "remote not found"):
		return "", errors.ErrRemoteNotFound
	default:
		return "", fmt.Errorf("%s: %w", out, err)
	}
}
