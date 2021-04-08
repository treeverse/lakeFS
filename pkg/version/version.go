package version

var (
	UnreleasedVersion = "dev"
	// Version is the current git version of the code.  It is filled in by "make build".
	// Make sure to change that target in Makefile if you change its name or package.
	Version = "dev"
)
