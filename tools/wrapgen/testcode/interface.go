//go:generate go run github.com/treeverse/lakefs/tools/wrapgen --package testcode --output ./wrapped_gen.go --interface Arithmetic ./interface.go
// Must run goimports after wrapgen: it adds unused imports.
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w ./wrapped_gen.go

// Package testcode contains code and generated code for testing wrapgen.
// It will only give correct results if "go generate" has run on it, so CI
// must do that outside of the test.
package testcode

type Arithmetic interface {
	Add(a, b int) int
	// Double returns an error, so it should be wrapped to measure its
	// duration.
	Double(a int) (int, error)
}
