package config

import (
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/stats"
)

const DefaultInstallationID = "anon@example.com"

func GetInstallationID(authService auth.Service) string {
	user, err := authService.GetFirstUser()
	if err != nil {
		return DefaultInstallationID
	}
	return user.Email
}

func GetStats(conf *Config, installationID string) *stats.BufferedCollector {
	sender := stats.NewDummySender()
	if conf.GetStatsEnabled() {
		sender = stats.NewHTTPSender(installationID, uuid.New().String(), conf.GetStatsAddress(), time.Now)
	}
	return stats.NewBufferedCollector(
		stats.WithSender(sender),
		stats.WithFlushInterval(conf.GetStatsFlushInterval()))
}
