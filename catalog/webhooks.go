package catalog

type HookDocument struct {
	Name        string          `yaml:"name"`
	Description string          `yaml:"description"`
	On          HookOn          `yaml:"on"`
	Hooks       map[string]Hook `yaml:"hooks"`
}
type HookOnAction struct {
	Branches []string `yaml:"branches"`
}
type HookOn struct {
	PreMerge  HookOnAction `yaml:"pre-merge"`
	PreCommit HookOnAction `yaml:"pre-commit"`
}
type HookProperties struct {
	URL string `yaml:"url"`
}
type Hook struct {
	Type        string         `yaml:"type"`
	Description string         `yaml:"description"`
	Properties  HookProperties `yaml:"properties"`
}
