package local

import (
	"errors"
	"regexp"
)

type ParsedGitURL struct {
	Domain string
	Name   string
	Org    string
}

var GitHubUrlPattern = regexp.MustCompile("(?P<domain>(github|gitlab|bitbucket)\\.com)[:\\/]+(?P<org>\\w+)\\/(?P<name>\\w+)\\.git\\/?$")
var ErrInvalidGithubUrl = errors.New("invalid github url")

func ParseGitUrl(raw string) (*ParsedGitURL, error) {
	match := GitHubUrlPattern.FindStringSubmatch(raw)
	if match == nil {
		return nil, ErrInvalidGithubUrl
	}
	result := make(map[string]string)
	for i, name := range GitHubUrlPattern.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	return &ParsedGitURL{
		Domain: result["domain"],
		Name:   result["name"],
		Org:    result["org"],
	}, nil
}
