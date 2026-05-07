package version

import (
	"strings"
)

var (
	UnreleasedVersion = "dev"
	// Version is the current git version of the code.  It is filled in by "make build".
	// Make sure to change that target in Makefile if you change its name or package.
	Version = "dev"
	// Distribution identifies community builds of lakectl. Empty string means
	// not a community build.
	Distribution = DistributionCommunity
)

const DistributionCommunity = "community"

func IsVersionUnreleased() bool {
	return strings.HasPrefix(Version, UnreleasedVersion) || strings.HasPrefix(Version, "sha-")
}
