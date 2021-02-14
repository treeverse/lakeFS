// Helps generate coverage for main pretending it is really a test.  Otherwise NO COVERAGE FOR
// YOU.  See https://blog.cloudflare.com/go-coverage-with-external-tests/
//
// +build buildcover

package main

import (
	"testing"
)

func TestRunMain(*testing.T) {
	main()
}
