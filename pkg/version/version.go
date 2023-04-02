package version

import (
	"fmt"
	"strings"

	goversion "github.com/hashicorp/go-version"
)

var (
	UnreleasedVersion = "dev"
	// Version is the current git version of the code.  It is filled in by "make build".
	// Make sure to change that target in Makefile if you change its name or package.
	Version = "dev"
)

func IsVersionUnreleased() bool {
	return strings.HasPrefix(Version, UnreleasedVersion)
}
func CheckLatestVersion(targetVersion string) (*LatestVersionResponse, error) {
	targetV, err := goversion.NewVersion(targetVersion)

	if err != nil {
		return nil, fmt.Errorf("tag parse %s: %w", targetVersion, err)
	}

	if IsVersionUnreleased() {
		return &LatestVersionResponse{
			Outdated:       true,
			LatestVersion:  targetV.String(),
			CurrentVersion: Version,
		}, nil
	}

	currentV, err := goversion.NewVersion(Version)

	if err != nil {
		return nil, fmt.Errorf("version parse %s: %w", Version, err)
	}

	return &LatestVersionResponse{
		Outdated:       targetV.LessThan(currentV),
		LatestVersion:  targetV.String(),
		CurrentVersion: currentV.String(),
	}, nil
}
