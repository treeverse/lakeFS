package catalog

import (
	"testing"

	"github.com/go-test/deep"

	"gopkg.in/yaml.v3"
)

func TestParseDocument(t *testing.T) {
	const data = `
name: Good merge
description: set of checks to verify that branch is good
on:
  pre-commit:
    branches:
      - feature-*
  pre-merge:
    branches:
      - master
      - stage
hooks:
  no_temp:
    type: webhook
    description: checking no temporary files found
    properties:
      url: "https://api.lakefs.io/webhook?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R"
  no_freeze:
    type: webhook
    description: check production is not in dev freeze
    properties:
      url: "https://api.lakefs.io/webhook?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R"
`
	var doc HookDocument
	err := yaml.Unmarshal([]byte(data), &doc)
	if err != nil {
		t.Fatal("Failed to unmarshal valid configuration:", err)
	}
	expected := HookDocument{
		Name:        "Good merge",
		Description: "set of checks to verify that branch is good",
		On: HookOn{
			PreMerge: HookOnAction{
				Branches: []string{"master", "stage"},
			},
			PreCommit: HookOnAction{
				Branches: []string{"feature-*"},
			},
		},
		Hooks: map[string]Hook{
			"no_temp": {
				Type:        "webhook",
				Description: "checking no temporary files found",
				Properties: HookProperties{
					URL: "https://api.lakefs.io/webhook?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R",
				},
			},
			"no_freeze": {
				Type:        "webhook",
				Description: "check production is not in dev freeze",
				Properties: HookProperties{
					URL: "https://api.lakefs.io/webhook?t=1za2PbkZK1bd4prMuTDr6BeEQwWYcX2R",
				},
			},
		},
	}
	if diff := deep.Equal(doc, expected); diff != nil {
		t.Fatal("HookDocument found diff", diff)
	}
}
