package cmd

import (
	"errors"
	"time"
)

const (
	defaultPollInterval = 3 * time.Second // default interval while pulling tasks status
	minimumPollInterval = time.Second     // minimum interval while pulling tasks status
	defaultPollTimeout  = time.Hour       // default expiry for pull status with no update
)

var ErrTaskNotCompleted = errors.New("task not completed")
